// build: gcc -O2 -Wall -o server server_uring.c -luring -lpthread
// needs: liburing-dev (apt install liburing-dev), kernel 5.19+ for multishot
// accept
#include <liburing.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static const unsigned char RESP[] = "HTTP/1.1 200 OK\r\n"
                                    "Content-Length: 5\r\n"
                                    "Connection: keep-alive\r\n"
                                    "\r\n"
                                    "PONG!";
static const size_t RESP_LEN = sizeof RESP - 1;

enum op { OP_ACCEPT, OP_RECV, OP_SEND };

typedef struct conn {
  int fd;
  enum op op;
  size_t send_off;
  char buf[16385];
} conn_t;

static struct io_uring_sqe *get_sqe(struct io_uring *ring) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    io_uring_submit(ring);
    sqe = io_uring_get_sqe(ring);
  }
  return sqe;
}

static void arm_accept(struct io_uring *ring, conn_t *ac) {
  struct io_uring_sqe *sqe = get_sqe(ring);
  io_uring_prep_multishot_accept(sqe, ac->fd, NULL, NULL, 0);
  io_uring_sqe_set_data(sqe, ac);
}

static void arm_recv(struct io_uring *ring, conn_t *c) {
  c->op = OP_RECV;
  struct io_uring_sqe *sqe = get_sqe(ring);
  io_uring_prep_recv(sqe, c->fd, c->buf, sizeof(c->buf) - 1, 0);
  io_uring_sqe_set_data(sqe, c);
}

static void arm_send(struct io_uring *ring, conn_t *c) {
  c->op = OP_SEND;
  struct io_uring_sqe *sqe = get_sqe(ring);
  io_uring_prep_send(sqe, c->fd, RESP + c->send_off, RESP_LEN - c->send_off, 0);
  io_uring_sqe_set_data(sqe, c);
}

static void *worker(void *arg) {
  (void)arg;
  int lfd = socket(AF_INET6, SOCK_STREAM, 0);
  if (lfd < 0) {
    perror("socket");
    return NULL;
  }
  int one = 1, v6 = 0;
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
  setsockopt(lfd, IPPROTO_IPV6, IPV6_V6ONLY, &v6, sizeof v6);
  struct sockaddr_in6 a = {.sin6_family = AF_INET6,
                           .sin6_port = htons(8888),
                           .sin6_addr = IN6ADDR_ANY_INIT};
  if (bind(lfd, (struct sockaddr *)&a, sizeof a) < 0) {
    perror("bind");
    return NULL;
  }
  listen(lfd, 1024);
  // ponytail: fds stay blocking-mode; io_uring dispatches accept/recv/send
  // async internally so O_NONBLOCK (needed for epoll) isn't required here.

  struct io_uring ring;
  if (io_uring_queue_init(4096, &ring, 0) < 0) {
    perror("io_uring_queue_init");
    return NULL;
  }

  conn_t accept_conn = {.fd = lfd, .op = OP_ACCEPT};
  arm_accept(&ring, &accept_conn);

  for (;;) {
    io_uring_submit_and_wait(&ring, 1);
    unsigned head, count = 0;
    struct io_uring_cqe *cqe;
    io_uring_for_each_cqe(&ring, head, cqe) {
      count++;
      conn_t *c = io_uring_cqe_get_data(cqe);

      if (c->op == OP_ACCEPT) {
        if (cqe->res >= 0) {
          conn_t *nc = malloc(sizeof *nc);
          if (!nc) {
            close(cqe->res);
          } else {
            // Disable Nagle: tiny request/response ping-pong stalls ~40ms
            // against delayed ACK otherwise (invisible on loopback, fatal
            // across a NIC).
            int one = 1;
            setsockopt(cqe->res, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            nc->fd = cqe->res;
            nc->send_off = 0;
            arm_recv(&ring, nc);
          }
        }
        if (!(cqe->flags & IORING_CQE_F_MORE))
          arm_accept(&ring, &accept_conn);
        continue;
      }

      if (c->op == OP_RECV) {
        if (cqe->res <= 0) {
          close(c->fd);
          free(c);
          continue;
        }
        c->buf[cqe->res] = 0;
        // ponytail: assumes the full request lands in one recv, same
        // limit as the original epoll version. upgrade: accumulate
        // across reads and track header end if that stops holding.
        if (strstr(c->buf, "\r\n\r\n")) {
          c->send_off = 0;
          arm_send(&ring, c);
        } else
          arm_recv(&ring, c);
        continue;
      }

      // OP_SEND
      if (cqe->res < 0) {
        close(c->fd);
        free(c);
        continue;
      }
      c->send_off += (size_t)cqe->res;
      if (c->send_off < RESP_LEN)
        arm_send(&ring, c);
      else
        arm_recv(&ring, c);
    }
    io_uring_cq_advance(&ring, count);
  }
}

int main(void) {
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n < 1)
    n = 4;
  pthread_t th[1024];
  for (long i = 0; i < n; i++)
    if (pthread_create(&th[i], NULL, worker, NULL) != 0) {
      perror("pthread_create");
      return 1;
    }
  for (long i = 0; i < n; i++)
    pthread_join(th[i], NULL);
  return 0;
}
