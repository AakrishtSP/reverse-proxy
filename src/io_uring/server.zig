//! io_uring-backed HTTP/1.1 proxy loop — minimal first cut.
//!
//! Single thread, one outstanding request per connection at a time (submitted
//! to the ring and reaped on a CQE), non-blocking sockets. Backend is a fresh
//! connection per request asked to close; the client hop is keep-alive for
//! Content-Length-framed responses (the `Connection: close` the backend emits
//! is stripped so the client can stay alive), and close-framed otherwise.
//!
//! Scope decisions for this cut (see plan.md §2):
//!   - chunked request bodies are rejected (415)
//!   - no backend connection pool / reuse yet
//!   - GET-focused relay; request bodies are forwarded but not reclaimed
//! Upgrade path is full parity with epoll (backend pool, chunked framing).

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const net = std.Io.net;

const http = @import("../http/parser.zig");
const Parser = http.Parser;
const Request = http.Request;
const util = @import("../http/util.zig");

const Router = @import("../router.zig").Router;
const IoUringConfig = @import("../config.zig").IoUringConfig;
const log = @import("../log.zig");

const buf_size = 8192;
const resp_buf_size = 65536;
// user_data for the pending accept op. All-ones: a connection tag always
// carries a small slot index in its low 32 bits, so this never collides.
const acceptable_magic: u64 = std.math.maxInt(u64);
// Cap on consecutive -EAGAIN reissues of one op before the connection is
// dropped: a transiently-full socket buffer clears in a few spins (~100ns
// each); a peer that never becomes ready must not pin the loop.
const max_eagain: u8 = 100;

fn isAgain(res: i32) bool {
    return res == -@as(i32, @intCast(@intFromEnum(linux.E.AGAIN)));
}

const State = enum {
    read_head, // client IN: parse a request head
    read_body, // client IN: more Content-Length request body
    connecting, // backend connect CQE pending
    send_req, // backend OUT: request head in flight
    send_body, // backend OUT: request body in flight
    read_resp, // backend IN: accumulate + frame response
    relay, // client OUT: drain resp toward the client
    closing, // client OUT: one-shot error response, then teardown
};

const Sockaddr = extern union {
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,
};

const Conn = struct {
    client: posix.fd_t,
    backend: ?posix.fd_t = null,
    backend_addr: net.IpAddress = undefined,
    backend_sa: Sockaddr = undefined,
    backend_sa_len: posix.socklen_t = 0,
    pooled_socket: bool = false, // backend came from the pool (stale-retry allowed)
    stale_retried: bool = false,
    gen: u32 = 0, // slot generation: stale ring CQEs from a previous
    // occupant carry an older gen and are ignored on dispatch
    eagain_streak: u8 = 0, // consecutive -EAGAIN rounds (bounded; see onOp)
    polling: bool = false, // a readiness POLL_ADD (not an I/O op) is outstanding

    state: State = .read_head,

    // inbound client window (head, then body)
    inbox: [buf_size]u8 = undefined,
    in_start: usize = 0,
    in_end: usize = 0,
    parser: Parser = .{},
    req: ?Request = null,
    body_left: u64 = 0,
    client_keep: bool = false,

    // rebuilt backend request head
    head: [buf_size]u8 = undefined,
    head_end: usize = 0,
    head_sent: usize = 0,

    // backend response accumulation + relay window
    resp: [resp_buf_size]u8 = undefined,
    resp_end: usize = 0, // bytes read from backend
    resp_sent: usize = 0, // bytes sent to client
    resp_head_end: usize = 0, // first byte after "\r\n\r\n", once found
    resp_head_done: bool = false,
    resp_cl: ?u64 = null,
    resp_close_framed: bool = false, // no framing -> read to EOF
    resp_conn_close: bool = false, // backend asked to close -> don't pool
};

const ConnPool = struct {
    slots: []Conn,
    free: []usize,
    top: usize = 0,

    fn init(allocator: std.mem.Allocator, n: usize) !ConnPool {
        const slots = try allocator.alloc(Conn, n);
        errdefer allocator.free(slots);
        const free = try allocator.alloc(usize, n);
        var pool = ConnPool{
            .slots = slots,
            .free = free,
        };
        var i: usize = 0;
        while (i < n) : (i += 1) {
            pool.free[i] = i;
            pool.top += 1;
        }
        return pool;
    }
    fn deinit(self: *ConnPool, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.free);
    }
    fn acquire(self: *ConnPool) ?usize {
        if (self.top == 0) return null;
        self.top -= 1;
        return self.free[self.top];
    }
    fn indexOf(self: *ConnPool, conn: *Conn) usize {
        const base = @intFromPtr(self.slots.ptr);
        return (@intFromPtr(conn) - base) / @sizeOf(Conn);
    }
    /// Ring user_data tag for a slot: generation (high 32) + slot index.
    /// A CQE arriving after the slot was recycled carries a stale gen and
    /// is dropped on dispatch instead of corrupting the new occupant.
    fn tag(self: *ConnPool, conn: *Conn) u64 {
        return (@as(u64, conn.gen) << 32) | @as(u64, @intCast(self.indexOf(conn)));
    }
    fn release(self: *ConnPool, conn: *Conn) void {
        self.free[self.top] = self.indexOf(conn);
        self.top += 1;
    }
};

/// Shared pool of idle keep-alive backend sockets, borrowed per request and
/// returned at a clean framing boundary. Single-threaded loop, no locking.
const Pool = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayListUnmanaged(Item) = .empty,
    cap: usize,

    const Item = struct { fd: posix.fd_t, addr: net.IpAddress };

    fn init(allocator: std.mem.Allocator, cap: usize) Pool {
        return .{ .allocator = allocator, .cap = cap };
    }

    fn borrow(self: *Pool, addr: net.IpAddress) ?posix.fd_t {
        var i = self.items.items.len;
        while (i > 0) {
            i -= 1;
            if (std.meta.eql(self.items.items[i].addr, addr)) {
                return self.items.swapRemove(i).fd;
            }
        }
        return null;
    }

    fn release(self: *Pool, fd: posix.fd_t, addr: net.IpAddress) void {
        if (self.items.items.len >= self.cap) {
            sysClose(fd);
            return;
        }
        self.items.append(self.allocator, .{ .fd = fd, .addr = addr }) catch sysClose(fd);
    }

    fn deinit(self: *Pool) void {
        for (self.items.items) |it| sysClose(it.fd);
        self.items.deinit(self.allocator);
    }
};

const Loop = struct {
    ring: linux.IoUring,
    router: *Router,
    listen_fd: posix.fd_t,
    conns: ConnPool,
    pool: Pool,
    opts: IoUringConfig,
    multishot_ok: bool = true, // cleared if the kernel rejects multishot accept
    accept_seen: bool = false, // any successful accept yet (tells EINVAL apart)

    // ---- submit helpers (each queues one op; flushed by run's main loop) ----

    /// One standing multishot accept (pre-5.19 kernels fall back to re-armed
    /// one-shot: see acceptReady). Re-armed by the event loop only when a CQE
    /// arrives without IORING_CQE_F_MORE, i.e. the multishot terminated.
    /// Sockets stay nonblocking: a momentarily-unready op completes with
    /// -EAGAIN, which onOp turns into a bounded reissue (see eagain_streak)
    /// instead of tearing the connection down.
    fn submitAccept(self: *Loop) void {
        const flags = posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK;
        if (self.multishot_ok) {
            _ = self.ring.accept_multishot(acceptable_magic, self.listen_fd, null, null, flags) catch {
                return;
            };
        } else {
            _ = self.ring.accept(acceptable_magic, self.listen_fd, null, null, flags) catch {
                return;
            };
        }
    }

    fn submitRecv(self: *Loop, conn: *Conn, fd: posix.fd_t, buf: []u8) void {
        _ = self.ring.recv(self.conns.tag(conn), fd, .{ .buffer = buf }, 0) catch {
            self.teardown(conn);
            return;
        };
    }

    fn submitSend(self: *Loop, conn: *Conn, fd: posix.fd_t, buf: []const u8) void {
        _ = self.ring.send(self.conns.tag(conn), fd, buf, 0) catch {
            self.teardown(conn);
            return;
        };
    }

    fn submitConnect(self: *Loop, conn: *Conn) void {
        _ = self.ring.connect(self.conns.tag(conn), conn.backend.?, @ptrCast(&conn.backend_sa), conn.backend_sa_len) catch {
            self.teardown(conn);
            return;
        };
    }

    // ---- accept ----

    fn acceptReady(self: *Loop, res: i32) void {
        if (res < 0) {
            const err = posix.errno(@intCast(-res));
            if (!self.accept_seen and err == .INVAL and self.multishot_ok) {
                // Kernel predates multishot accept (5.19): fall back to
                // re-armed one-shot accepts. The event loop re-arms below
                // since this CQE carries no MORE flag.
                self.multishot_ok = false;
                return;
            }
            log.fmt("accept failed: {any}", .{err});
            return;
        }
        self.accept_seen = true;
        const fd: posix.fd_t = @intCast(res);
        const idx = self.conns.acquire() orelse {
            sysClose(fd); // no slots: refuse rather than queue
            return;
        };
        const conn = &self.conns.slots[idx];
        // New generation: any ring op still in flight from the slot's
        // previous occupant dispatches with a stale gen and is ignored.
        const gen = conn.gen +% 1;
        conn.* = .{ .client = fd };
        conn.gen = gen;
        enableTcpNoDelay(fd);
        conn.state = .read_head;
        self.submitRecv(conn, conn.client, conn.inbox[0..]);
    }

    // ---- per-op dispatch ----

    fn onOp(self: *Loop, conn: *Conn, res: i32) void {
        // Completion of a readiness poll: reissue the op; a poll error means
        // the fd is gone, so drop the connection.
        if (conn.polling) {
            conn.polling = false;
            if (res < 0) {
                self.teardown(conn);
                return;
            }
            self.reissue(conn);
            return;
        }
        // Momentarily-unready socket (-EAGAIN on a nonblocking fd): wait for
        // readiness in the kernel instead of spinning or tearing the
        // connection down. Bounded by eagain_streak: a peer that never
        // becomes ready is dropped rather than waited on forever.
        if (isAgain(res)) {
            if (conn.eagain_streak >= max_eagain) {
                self.teardown(conn);
                return;
            }
            conn.eagain_streak += 1;
            self.pollFor(conn);
            return;
        }
        conn.eagain_streak = 0;
        switch (conn.state) {
            .read_head => self.opReadHead(conn, res),
            .read_body => self.opReadBody(conn, res),
            .connecting => self.opConnect(conn, res),
            .send_req => self.opSendReq(conn, res),
            .send_body => self.opSendBody(conn, res),
            .read_resp => self.opReadResp(conn, res),
            .relay => self.opRelay(conn, res),
            .closing => self.teardown(conn),
        }
    }

    /// Submits a readiness poll for the op the connection is waiting on. The
    /// state always describes the outstanding op (see relayProgress), so the
    /// (fd, direction) pair derives from it. The poll CQE re-arms the op via
    /// reissue — no userspace spinning in between.
    fn pollFor(self: *Loop, conn: *Conn) void {
        const target: struct { fd: posix.fd_t, mask: u32 } = switch (conn.state) {
            .read_head, .read_body => .{ .fd = conn.client, .mask = linux.POLL.IN },
            .connecting => .{ .fd = conn.backend.?, .mask = linux.POLL.OUT },
            .send_req, .send_body => .{ .fd = conn.backend.?, .mask = linux.POLL.OUT },
            .read_resp => .{ .fd = conn.backend.?, .mask = linux.POLL.IN },
            .relay => .{ .fd = conn.client, .mask = linux.POLL.OUT },
            .closing => {
                self.teardown(conn); // error reply; don't wait, just drop
                return;
            },
        };
        _ = self.ring.poll_add(self.conns.tag(conn), target.fd, target.mask) catch {
            self.teardown(conn);
            return;
        };
        conn.polling = true;
    }

    /// Re-submits the op the connection is currently waiting on, without
    /// consuming anything: -EAGAIN moved no bytes and changed no state, so
    /// repeating the last submit is exactly correct. Every branch mirrors
    /// the submit its state's handler performed.
    fn reissue(self: *Loop, conn: *Conn) void {
        switch (conn.state) {
            .read_head, .read_body => self.submitRecv(conn, conn.client, conn.inbox[conn.in_end..]),
            .connecting => self.submitConnect(conn),
            .send_req => self.submitSend(conn, conn.backend.?, conn.head[conn.head_sent..conn.head_end]),
            .send_body => {
                if (conn.in_start < conn.in_end) {
                    const want = @min(conn.body_left, conn.in_end - conn.in_start);
                    self.submitSend(conn, conn.backend.?, conn.inbox[conn.in_start .. conn.in_start + @as(usize, @intCast(want))]);
                } else {
                    self.submitRecv(conn, conn.client, conn.inbox[conn.in_end..]);
                }
            },
            .read_resp => self.submitRecv(conn, conn.backend.?, conn.resp[conn.resp_end..]),
            .relay => self.relayProgress(conn),
            .closing => self.teardown(conn), // error reply; don't spin, just drop
        }
    }

    // -- request half --

    fn opReadHead(self: *Loop, conn: *Conn, res: i32) void {
        if (res <= 0) {
            self.teardown(conn); // error or client closed
            return;
        }
        var n: usize = @intCast(res);
        if (conn.in_end + n > conn.inbox.len) n = conn.inbox.len - conn.in_end;
        conn.in_end += n;

        const req = conn.parser.next(conn.inbox[0..conn.in_end]) catch {
            self.sendSimple(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        };
        if (req) |full| {
            self.startRequest(conn, full);
            return;
        }
        if (conn.in_end == conn.inbox.len) {
            self.sendSimple(conn, "HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        }
        self.submitRecv(conn, conn.client, conn.inbox[conn.in_end..]);
    }

    fn startRequest(self: *Loop, conn: *Conn, req: Request) void {
        conn.req = req;
        if (req.chunked) {
            self.sendSimple(conn, "HTTP/1.1 415 Unsupported Media Type\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        }
        conn.in_start = req.consumed;
        conn.body_left = req.content_length orelse 0;
        conn.client_keep = req.keep_alive and self.opts.client_keepalive;

        self.buildHead(conn, req) catch {
            self.teardown(conn);
            return;
        };
        const host_header = req.header("host") orelse {
            self.sendSimple(conn, "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        };
        conn.backend_addr = (self.router.route(util.hostOnly(host_header)) orelse {
            self.teardown(conn); // deny by closing, like nginx 444
            return;
        }).pick();

conn.backend_sa_len = buildSockaddr(conn.backend_addr, &conn.backend_sa);
        // Reuse an idle pooled backend when allowed; otherwise open fresh.
        // A pooled socket the backend idle-closed is detected on the first
        // send and reconnected once (see opSendReq) — but only when nothing
        // of the request has gone out yet.
        if (self.opts.backend_keepalive) {
            if (self.pool.borrow(conn.backend_addr)) |fd| {
                conn.backend = fd;
                conn.pooled_socket = true;
                conn.stale_retried = false;
                conn.state = .send_req;
                self.submitSend(conn, fd, conn.head[0..conn.head_end]);
                return;
            }
        }
        conn.pooled_socket = false;
        conn.stale_retried = false;
        conn.backend = sysOpenBackend(conn.backend_sa_len) catch |err| {
            log.fmt("connect failed: {any}", .{err});
            self.sendSimple(conn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        };
        conn.state = .connecting;
        self.submitConnect(conn);
    }

    fn buildHead(self: *Loop, conn: *Conn, req: Request) !void {
        var h: usize = 0;
        h += (try std.fmt.bufPrint(conn.head[h..], "{s} {s} HTTP/1.1\r\n", .{ req.method, req.target })).len;
        for (req.headers) |hd| {
            if (std.ascii.eqlIgnoreCase(hd.name, "x-forwarded-for")) continue;
            h += (try std.fmt.bufPrint(conn.head[h..], "{s}: {s}\r\n", .{ hd.name, hd.value })).len;
        }
        // Ask the backend to keep the socket alive only when the pool will
        // actually reuse it; otherwise every request pays idle-socket
        // bookkeeping on both ends for no payoff.
        const keep_backend = self.opts.backend_keepalive;
        h += (try std.fmt.bufPrint(conn.head[h..], "Connection: {s}\r\n\r\n", .{if (keep_backend) "keep-alive" else "close"})).len;
        conn.head_end = h;
        conn.head_sent = 0;
    }

    fn opConnect(self: *Loop, conn: *Conn, res: i32) void {
        if (res < 0) {
            log.fmt("connect failed: {any}", .{posix.errno(@intCast(-res))});
            self.sendSimple(conn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        }
        conn.state = .send_req;
        self.submitSend(conn, conn.backend.?, conn.head[0..conn.head_end]);
    }

    fn opSendReq(self: *Loop, conn: *Conn, res: i32) void {
        if (res < 0) {
            self.handleSendError(conn);
            return;
        }
        conn.head_sent += @intCast(res);
        if (conn.head_sent < conn.head_end) {
            self.submitSend(conn, conn.backend.?, conn.head[conn.head_sent..conn.head_end]);
            return;
        }
        // head fully sent: move on to body (or straight to response read)
        const available = conn.in_end - conn.in_start;
        if (conn.body_left == 0) {
            conn.state = .read_resp;
            self.submitRecv(conn, conn.backend.?, conn.resp[0..]);
            return;
        }
        if (conn.in_start < conn.in_end) {
            conn.state = .send_body;
            const want = @min(@as(usize, @intCast(conn.body_left)), available);
            self.submitSend(conn, conn.backend.?, conn.inbox[conn.in_start .. conn.in_start + want]);
            return;
        }
        conn.state = .read_body;
        self.submitRecv(conn, conn.client, conn.inbox[conn.in_end..]);
    }

    /// Stale pooled socket: the backend closed it while idle. Reconnect and
    /// re-send only when nothing of the request has gone out yet (the usual
    /// idle-close case, and the only one where re-sending is well-defined).
    fn handleSendError(self: *Loop, conn: *Conn) void {
        if (conn.pooled_socket and !conn.stale_retried and conn.head_sent == 0) {
            conn.stale_retried = true;
            if (conn.backend) |fd| sysClose(fd);
            conn.backend = sysOpenBackend(conn.backend_sa_len) catch {
                self.teardown(conn);
                return;
            };
            conn.pooled_socket = false;
            conn.head_sent = 0;
            conn.state = .connecting;
            self.submitConnect(conn);
            return;
        }
        self.teardown(conn);
    }

    fn opSendBody(self: *Loop, conn: *Conn, res: i32) void {
        if (res < 0) {
            self.teardown(conn);
            return;
        }
        conn.in_start += @intCast(res);
        conn.body_left -= @intCast(res);
        if (conn.body_left == 0) {
            conn.state = .read_resp;
            self.submitRecv(conn, conn.backend.?, conn.resp[0..]);
            return;
        }
        if (conn.in_start < conn.in_end) {
            const want = @min(conn.body_left, conn.in_end - conn.in_start);
            self.submitSend(conn, conn.backend.?, conn.inbox[conn.in_start .. conn.in_start + @as(usize, @intCast(want))]);
            return;
        }
        conn.state = .read_body;
        self.submitRecv(conn, conn.client, conn.inbox[conn.in_end..]);
    }

    fn opReadBody(self: *Loop, conn: *Conn, res: i32) void {
        if (res <= 0) {
            self.teardown(conn);
            return;
        }
        conn.in_end += @intCast(res);
        if (conn.in_start < conn.in_end and conn.body_left > 0) {
            conn.state = .send_body;
            const want = @min(conn.body_left, conn.in_end - conn.in_start);
            self.submitSend(conn, conn.backend.?, conn.inbox[conn.in_start .. conn.in_start + @as(usize, @intCast(want))]);
            return;
        }
        if (conn.body_left > 0) {
            self.submitRecv(conn, conn.client, conn.inbox[conn.in_end..]);
            return;
        }
        conn.state = .read_resp;
        self.submitRecv(conn, conn.backend.?, conn.resp[0..]);
    }

    // -- response half --

    fn opReadResp(self: *Loop, conn: *Conn, res: i32) void {
        if (res < 0) {
            self.teardown(conn);
            return;
        }
        if (res == 0) {
            self.finishResp(conn); // backend EOF
            return;
        }
        var n: usize = @intCast(res);
        if (conn.resp_end + n > conn.resp.len) n = conn.resp.len - conn.resp_end;
        conn.resp_end += n;
        self.frameAndRelay(conn);
    }

    fn frameAndRelay(self: *Loop, conn: *Conn) void {
        if (!conn.resp_head_done) {
            const head = std.mem.indexOf(u8, conn.resp[0..conn.resp_end], "\r\n\r\n") orelse {
                if (conn.resp_end == conn.resp.len) {
                    self.teardown(conn); // response head larger than buffer
                    return;
                }
                self.submitRecv(conn, conn.backend.?, conn.resp[conn.resp_end..]);
                return;
            };
            conn.resp_head_end = head + 4;
            conn.resp_cl = parseContentLength(conn.resp[0..conn.resp_head_end]);
            // close-framed when no Content-Length, or an explicit Connection: close
            conn.resp_conn_close =
                indexOfHeaderValue(conn.resp[0..conn.resp_head_end], "connection", "close");
            conn.resp_close_framed = conn.resp_cl == null or conn.resp_conn_close;
            conn.resp_head_done = true;
            conn.resp_sent = 0;
            if (!conn.resp_close_framed and conn.client_keep) {
                // rewrite "Connection: close" out of the head so the client can
                // reuse the connection (Content-Length still frames the body)
                const old_end = conn.resp_head_end;
                conn.resp_head_end = dropHeaderLine(conn.resp[0..conn.resp_end], conn.resp_head_end, conn.resp_end, "connection");
                conn.resp_end -= (old_end - conn.resp_head_end);
            } else if (!conn.client_keep and conn.resp_cl != null) {
                // The client connection closes after this response: it must
                // say so (HTTP/1.1 defaults to keep-alive). Otherwise the
                // client waits for a next response that never comes and only
                // learns the truth at EOF.
                const old_end = conn.resp_head_end;
                conn.resp_head_end = forceConnectionClose(conn.resp[0..], conn.resp_head_end, conn.resp_end);
                // Net change can go either way (dropped line vs added line).
                const delta: isize = @as(isize, @intCast(conn.resp_head_end)) - @as(isize, @intCast(old_end));
                conn.resp_end = @intCast(@as(isize, @intCast(conn.resp_end)) + delta);
            }
        }
        self.relayProgress(conn);
    }

    fn opRelay(self: *Loop, conn: *Conn, res: i32) void {
        if (res < 0) {
            self.teardown(conn);
            return;
        }
        conn.resp_sent += @intCast(res);
        self.relayProgress(conn);
    }

    /// Send as much buffered response as we have; decide read-more vs done.
    /// State always describes the outstanding op: .relay while a client send
    /// is in flight (its CQE must reach opRelay, which advances resp_sent),
    /// .read_resp while a backend recv is in flight.
    fn relayProgress(self: *Loop, conn: *Conn) void {
        if (conn.resp_close_framed) {
            if (conn.resp_sent < conn.resp_end) {
                conn.state = .relay;
                self.submitSend(conn, conn.client, conn.resp[conn.resp_sent..conn.resp_end]);
                return;
            }
            conn.state = .read_resp;
            self.submitRecv(conn, conn.backend.?, conn.resp[0..]); // wait for EOF
            return;
        }
        const target: usize = @intCast(conn.resp_head_end + (conn.resp_cl orelse unreachable));
        const have_cl = @min(conn.resp_end, target);
        if (conn.resp_sent < have_cl) {
            conn.state = .relay;
            self.submitSend(conn, conn.client, conn.resp[conn.resp_sent..have_cl]);
            return;
        }
        if (conn.resp_end < target) {
            conn.state = .read_resp;
            self.submitRecv(conn, conn.backend.?, conn.resp[conn.resp_end..]);
            return;
        }
        self.nextRequest(conn); // full response delivered
    }

    fn nextRequest(self: *Loop, conn: *Conn) void {
        // Backend socket: pool it when the response was cleanly framed and
        // the backend didn't ask to close; otherwise drop it. Either way it
        // no longer belongs to this client connection past this point.
        if (conn.backend) |fd| {
            if (self.opts.backend_keepalive and !conn.resp_conn_close) {
                self.pool.release(fd, conn.backend_addr);
            } else {
                sysClose(fd);
            }
            conn.backend = null;
        }
        conn.pooled_socket = false;
        conn.stale_retried = false;
        conn.resp_conn_close = false;
        if (!conn.client_keep) {
            self.teardown(conn);
            return;
        }
        conn.in_start = 0;
        conn.in_end = 0;
        conn.parser = .{};
        conn.req = null;
        conn.resp_end = 0;
        conn.resp_sent = 0;
        conn.resp_head_done = false;
        conn.resp_cl = null;
        conn.resp_close_framed = false;
        conn.state = .read_head;
        self.submitRecv(conn, conn.client, conn.inbox[0..]);
    }

    /// Drain a close-framed (just-closed) backend, then close both hops.
    fn finishResp(self: *Loop, conn: *Conn) void {
        if (!conn.resp_head_done) {
            self.teardown(conn); // connection died before a head
            return;
        }
        if (conn.resp_sent < conn.resp_end) {
            conn.state = .relay;
            self.submitSend(conn, conn.client, conn.resp[conn.resp_sent..conn.resp_end]);
            return;
        }
        conn.client_keep = false; // no framing -> cannot reuse client
        self.teardown(conn);
    }

    fn sendSimple(self: *Loop, conn: *Conn, msg: []const u8) void {
        // `msg` is a comptime string literal; static lifetime, safe to send async
        conn.state = .closing;
        self.submitSend(conn, conn.client, msg);
    }

    fn teardown(self: *Loop, conn: *Conn) void {
        if (conn.backend) |fd| sysClose(fd);
        sysClose(conn.client);
        self.conns.release(conn);
    }
};

// ---- response head parsing helpers ----

fn parseContentLength(head: []const u8) ?u64 {
    var it = std.mem.splitSequence(u8, head, "\r\n");
    while (it.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(line[0..colon], "content-length")) {
            return std.fmt.parseUnsigned(u64, std.mem.trim(u8, line[colon + 1 ..], " \t"), 10) catch null;
        }
    }
    return null;
}

fn indexOfHeaderValue(head: []const u8, name: []const u8, needle: []const u8) bool {
    var it = std.mem.splitSequence(u8, head, "\r\n");
    while (it.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(line[0..colon], name)) {
            if (std.ascii.indexOfIgnoreCase(line[colon + 1 ..], needle) != null) return true;
        }
    }
    return false;
}

/// Ensures the buffered head tells the client this connection closes:
/// drops any Connection line, then appends "Connection: close" *before* the
/// terminal blank line. `buf` is the whole response buffer (for capacity),
/// `head_end`/`buf_end` mark the head and the buffered bytes; body bytes
/// shift along. Returns the new head length; the caller must grow its
/// buffered length by the difference.
fn forceConnectionClose(buf: []u8, head_end: usize, buf_end: usize) usize {
    const h = dropHeaderLine(buf[0..buf_end], head_end, buf_end, "connection");
    const add = "Connection: close\r\n";
    // The head ends with the blank line's CRLF; the new header goes before
    // it so framing stays "headers, blank, body".
    const at = h - 2;
    if (buf_end + add.len > buf.len) return h; // no room; leave dropped-only
    std.mem.copyBackwards(u8, buf[at + add.len .. buf_end + add.len], buf[at..buf_end]);
    @memcpy(buf[at .. at + add.len], add);
    return h + add.len;
}

/// Removes a single case-insensitive header line (e.g. "Connection: close")
/// from the buffered head, shifting everything after it left — including any
/// body bytes already read past the head terminator. Returns the new head length.
fn dropHeaderLine(head: []u8, head_end: usize, buf_end: usize, name: []const u8) usize {
    const first = std.mem.indexOf(u8, head[0..head_end], "\r\n") orelse return head_end;
    var line_start = first + 2; // skip the status line
    while (line_start < head_end) {
        const eol = std.mem.indexOfPos(u8, head[0..head_end], line_start, "\r\n") orelse return head_end;
        const line = head[line_start..eol];
        if (line.len > 0) {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse return head_end;
            if (std.ascii.eqlIgnoreCase(line[0..colon], name)) {
                const removed = (eol + 2) - line_start;
                std.mem.copyForwards(u8, head[line_start .. buf_end - removed], head[eol + 2 .. buf_end]);
                return head_end - removed;
            }
        }
        line_start = eol + 2;
    }
    return head_end;
}

// ---- raw syscall / sockaddr helpers (mirror epoll/threaded) ----

fn check(rc: usize) !void {
    return switch (posix.errno(rc)) {
        .SUCCESS => {},
        .AGAIN => error.WouldBlock,
        .PIPE => error.BrokenPipe,
        .CONNREFUSED => error.ConnectionRefused,
        else => |e| posix.unexpectedErrno(e),
    };
}

fn sysSocket(domain: u32, socket_type: u32, protocol: u32) !posix.fd_t {
    const rc = linux.socket(domain, socket_type, protocol);
    try check(rc);
    return @intCast(rc);
}

fn sysClose(fd: posix.fd_t) void {
    _ = linux.close(fd);
}

fn enableTcpNoDelay(fd: posix.fd_t) void {
    posix.setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};
}

/// Fills an outgoing sockaddr for a backend address; returns its length.
fn buildSockaddr(addr: net.IpAddress, sa: *Sockaddr) posix.socklen_t {
    switch (addr) {
        .ip4 => |ip4| {
            sa.* = .{ .in = .{
                .family = posix.AF.INET,
                .port = std.mem.nativeToBig(u16, ip4.port),
                .addr = @bitCast(ip4.bytes),
                .zero = [_]u8{0} ** 8,
            } };
            return @sizeOf(posix.sockaddr.in);
        },
        .ip6 => |ip6| {
            sa.* = .{ .in6 = .{
                .family = posix.AF.INET6,
                .port = std.mem.nativeToBig(u16, ip6.port),
                .flowinfo = 0,
                .addr = ip6.bytes,
                .scope_id = 0,
            } };
            return @sizeOf(posix.sockaddr.in6);
        },
    }
}

/// Opens a fresh nonblocking socket for the backend; the actual connect()
/// happens as a ring op (submitConnect), so no connect syscall here.
fn sysOpenBackend(len: posix.socklen_t) !posix.fd_t {
    const domain: u32 = if (len == @sizeOf(posix.sockaddr.in)) posix.AF.INET else posix.AF.INET6;
    const fd = try sysSocket(domain, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    errdefer sysClose(fd);
    enableTcpNoDelay(fd);
    return fd;
}

pub fn run(allocator: std.mem.Allocator, router: *Router, port: u16, opts: IoUringConfig) !void {
    var sigpipe_action: posix.Sigaction = .{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.PIPE, &sigpipe_action, null);

    // N independent event loops, each with its own SO_REUSEPORT listener,
    // ring, ConnPool partition and backend pool, so the single-core ceiling
    // scales across cores. The calling thread runs the last loop so run()
    // blocks forever.
    const n = @max(@as(usize, 1), opts.loops);
    for (0..n - 1) |_| {
        _ = std.Thread.spawn(.{}, loopWorker, .{ allocator, router, port, opts }) catch |err| {
            log.fmt("io_uring loop spawn failed: {any}", .{err});
        };
    }
    loopWorker(allocator, router, port, opts);
}

fn loopWorker(allocator: std.mem.Allocator, router: *Router, port: u16, opts: IoUringConfig) void {
    const nloops = @max(@as(usize, 1), opts.loops);

    // Ring depth is partitioned across loops (halved per doubling, floored
    // at 64) so N loops pin ~the same locked memory (RLIMIT_MEMLOCK) as one
    // loop; IoUring.init requires a power of two, which halving preserves.
    var per_loop_entries: u16 = opts.entries;
    var remaining = nloops;
    while (remaining > 1) : (remaining /= 2) per_loop_entries /= 2;
    per_loop_entries = @max(per_loop_entries, 64);

    // A kernel poll thread drains the submission queue instead of the
    // submit syscall; falls back to plain submit when the kernel refuses
    // (e.g. lacking privileges on older kernels).
    var ring = linux.IoUring.init(
        per_loop_entries,
        if (opts.sqpoll) linux.IORING_SETUP_SQPOLL else 0,
    ) catch |err| sqpoll_fallback: {
        if (!opts.sqpoll) {
            log.fmt("io_uring init failed: {any}", .{err});
            return;
        }
        log.fmt("io_uring sqpoll init failed ({any}); falling back to submit", .{err});
        break :sqpoll_fallback linux.IoUring.init(per_loop_entries, 0) catch |err2| {
            log.fmt("io_uring init failed: {any}", .{err2});
            return;
        };
    };
    defer ring.deinit();

    const listen_fd = sysSocket(posix.AF.INET6, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP) catch |err| {
        log.fmt("socket failed: {any}", .{err});
        return;
    };
    defer sysClose(listen_fd);
    posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch {};
    posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1))) catch {};
    posix.setsockopt(listen_fd, posix.IPPROTO.IPV6, posix.IPV6.V6ONLY, &std.mem.toBytes(@as(c_int, 0))) catch {};

    const bind_addr: posix.sockaddr.in6 = .{
        .family = posix.AF.INET6,
        .port = std.mem.nativeToBig(u16, port),
        .flowinfo = 0,
        .addr = [_]u8{0} ** 16,
        .scope_id = 0,
    };
    check(linux.bind(listen_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in6))) catch return;
    check(linux.listen(listen_fd, 4096)) catch return;

    // Total budgets split across loops; each loop refuses accepts / pools
    // within its own share (lock-free; remainder is dropped).
    var conns = ConnPool.init(allocator, @max(@as(usize, 1), opts.max_connections / nloops)) catch return;
    defer conns.deinit(allocator);

    var pool = Pool.init(allocator, @max(@as(usize, 1), opts.pool_size / nloops));
    defer pool.deinit();

    var loop = Loop{ .ring = ring, .router = router, .listen_fd = listen_fd, .conns = conns, .pool = pool, .opts = opts };
    loop.submitAccept();
    _ = loop.ring.submit() catch {};

    var cqes: [256]linux.io_uring_cqe = undefined;
    while (true) {
        const n = loop.ring.copy_cqes(&cqes, 1) catch |err| {
            log.fmt("copy_cqes failed: {any}", .{err});
            continue;
        };
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == acceptable_magic) {
                loop.acceptReady(cqe.res);
                // Multishot keeps firing on its own; only re-arm when it
                // terminated (no MORE flag). One-shot accepts never carry
                // MORE, so the fallback path re-arms every time.
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0) loop.submitAccept();
            } else {
                // Connection tag: slot index (low 32) + generation. A CQE
                // from before the slot's latest acquire carries a stale gen
                // (teardown with ring ops still in flight) and is dropped
                // instead of corrupting the new occupant.
                const idx: u32 = @truncate(cqe.user_data);
                if (@as(usize, idx) >= loop.conns.slots.len) continue; // cannot happen; defensive
                const conn = &loop.conns.slots[idx];
                if (conn.gen != @as(u32, @truncate(cqe.user_data >> 32))) continue;
                loop.onOp(conn, cqe.res);
            }
        }
        _ = loop.ring.submit() catch {};
    }
}

test "dropHeaderLine keeps already-read body bytes" {
    // Mirrors the bench backend's reply: head + 5 body bytes in one read.
    var buf: [128]u8 = undefined;
    const raw = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nPONG!";
    @memcpy(buf[0..raw.len], raw);
    const head_end = std.mem.indexOf(u8, buf[0..raw.len], "\r\n\r\n").? + 4;
    const new_head_end = dropHeaderLine(buf[0..raw.len], head_end, raw.len, "connection");
    try std.testing.expectEqual(head_end - "Connection: keep-alive\r\n".len, new_head_end);
    try std.testing.expectEqualStrings(
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPONG!",
        buf[0 .. new_head_end + 5],
    );
}

test "forceConnectionClose swaps keep-alive for close" {
    var buf: [128]u8 = undefined;
    const raw = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nPONG!";
    @memcpy(buf[0..raw.len], raw);
    const head_end = std.mem.indexOf(u8, buf[0..raw.len], "\r\n\r\n").? + 4;
    const new_head_end = forceConnectionClose(buf[0..], head_end, raw.len);
    try std.testing.expectEqualStrings(
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nPONG!",
        buf[0 .. new_head_end + 5],
    );
}
