#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

static const unsigned char RESP[] =
    "HTTP/1.1 200 OK\r\n"
    "Content-Length: 5\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "PONG!";
static const size_t RESP_LEN = sizeof RESP - 1;

static void *worker(void *arg) {
    (void)arg;
    int lfd = socket(AF_INET6, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return NULL; }
    int one = 1, v6 = 0;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
    setsockopt(lfd, IPPROTO_IPV6, IPV6_V6ONLY, &v6, sizeof v6);
    struct sockaddr_in6 a = {.sin6_family = AF_INET6,
                             .sin6_port = htons(8888),
                             .sin6_addr = IN6ADDR_ANY_INIT};
    if (bind(lfd, (struct sockaddr*)&a, sizeof a) < 0) { perror("bind"); return NULL; }
    listen(lfd, 1024);
    int lfl = fcntl(lfd, F_GETFL, 0);
    fcntl(lfd, F_SETFL, lfl | O_NONBLOCK);

    int ep = epoll_create1(0);
    if (ep < 0) { perror("epoll_create1"); return NULL; }
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = lfd};
    epoll_ctl(ep, EPOLL_CTL_ADD, lfd, &ev);

    struct epoll_event evs[1024];
    char buf[16385];
    for (;;) {
        int n = epoll_wait(ep, evs, 1024, -1);
        if (n < 0 && errno == EINTR) continue;
        for (int i = 0; i < n; i++) {
            int fd = evs[i].data.fd;
            if (fd == lfd) {
                for (;;) {
                    int c = accept(lfd, NULL, NULL);
                    if (c < 0) break;
                    int flags = fcntl(c, F_GETFL, 0);
                    fcntl(c, F_SETFL, flags | O_NONBLOCK);
                    struct epoll_event ce = {.events = EPOLLIN, .data.fd = c};
                    epoll_ctl(ep, EPOLL_CTL_ADD, c, &ce);
                }
            } else if (evs[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
                ssize_t r = read(fd, buf, sizeof buf - 1);
                if (r <= 0) { epoll_ctl(ep, EPOLL_CTL_DEL, fd, NULL); close(fd); continue; }
                buf[r] = 0;
                if (strstr(buf, "\r\n\r\n")) {
                    size_t off = 0;
                    while (off < RESP_LEN) {
                        ssize_t w = write(fd, RESP + off, RESP_LEN - off);
                        if (w < 0) { if (errno == EAGAIN) continue; break; }
                        off += (size_t)w;
                    }
                }
            }
        }
    }
}

int main(void) {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 4;
    pthread_t th[1024];
    for (long i = 0; i < n; i++)
        if (pthread_create(&th[i], NULL, worker, NULL) != 0) { perror("pthread_create"); return 1; }
    for (long i = 0; i < n; i++)
        pthread_join(th[i], NULL);
    return 0;
}
