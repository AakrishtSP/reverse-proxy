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
const acceptable_magic: u64 = 0; // user_data for the pending accept op

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
};

const ConnPool = struct {
    slots: []Conn,
    free: []usize,
    top: usize = 0,

    fn init(allocator: std.mem.Allocator, n: usize) !ConnPool {
        const free = try allocator.alloc(usize, n);
        var pool = ConnPool{
            .slots = try allocator.alloc(Conn, n),
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
    fn acquire(self: *ConnPool) ?*Conn {
        if (self.top == 0) return null;
        self.top -= 1;
        return &self.slots[self.free[self.top]];
    }
    fn release(self: *ConnPool, conn: *Conn) void {
        const base = @intFromPtr(self.slots.ptr);
        const idx = (@intFromPtr(conn) - base) / @sizeOf(Conn);
        self.free[self.top] = idx;
        self.top += 1;
    }
};

const Loop = struct {
    ring: linux.IoUring,
    router: *Router,
    listen_fd: posix.fd_t,
    conns: ConnPool,
    opts: IoUringConfig,

    // ---- submit helpers (each queues one op; flushed by run's main loop) ----

    fn submitAccept(self: *Loop) void {
        _ = self.ring.accept(acceptable_magic, self.listen_fd, null, null, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK) catch {
            return;
        };
    }

    fn submitRecv(self: *Loop, conn: *Conn, fd: posix.fd_t, buf: []u8) void {
        _ = self.ring.recv(@intFromPtr(conn), fd, .{ .buffer = buf }, 0) catch {
            self.teardown(conn);
            return;
        };
    }

    fn submitSend(self: *Loop, conn: *Conn, fd: posix.fd_t, buf: []const u8) void {
        _ = self.ring.send(@intFromPtr(conn), fd, buf, 0) catch {
            self.teardown(conn);
            return;
        };
    }

    fn submitConnect(self: *Loop, conn: *Conn) void {
        _ = self.ring.connect(@intFromPtr(conn), conn.backend.?, @ptrCast(&conn.backend_sa), conn.backend_sa_len) catch {
            self.teardown(conn);
            return;
        };
    }

    // ---- accept ----

    fn acceptReady(self: *Loop, res: i32) void {
        defer self.submitAccept(); // keep exactly one accept in flight
        if (res < 0) {
            log.fmt("accept failed: {any}", .{posix.errno(@intCast(-res))});
            return;
        }
        const fd: posix.fd_t = @intCast(res);
        const conn = self.conns.acquire() orelse {
            sysClose(fd); // no slots: refuse rather than queue
            return;
        };
        conn.* = .{ .client = fd };
        conn.state = .read_head;
        self.submitRecv(conn, conn.client, conn.inbox[0..]);
    }

    // ---- per-op dispatch ----

    fn onOp(self: *Loop, conn: *Conn, res: i32) void {
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
    conn.backend = sysConnect(@ptrCast(&conn.backend_sa), conn.backend_sa_len) catch |err| {
            log.fmt("connect failed: {any}", .{err});
            self.sendSimple(conn, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            return;
        };
        conn.state = .connecting;
        self.submitConnect(conn);
    }

    fn buildHead(self: *Loop, conn: *Conn, req: Request) !void {
        _ = self;
        var h: usize = 0;
        h += (try std.fmt.bufPrint(conn.head[h..], "{s} {s} HTTP/1.1\r\n", .{ req.method, req.target })).len;
        for (req.headers) |hd| {
            if (std.ascii.eqlIgnoreCase(hd.name, "x-forwarded-for")) continue;
            h += (try std.fmt.bufPrint(conn.head[h..], "{s}: {s}\r\n", .{ hd.name, hd.value })).len;
        }
        h += (try std.fmt.bufPrint(conn.head[h..], "Connection: close\r\n\r\n", .{})).len;
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
            self.teardown(conn);
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
            conn.resp_close_framed = conn.resp_cl == null or
                indexOfHeaderValue(conn.resp[0..conn.resp_head_end], "connection", "close");
            conn.resp_head_done = true;
            conn.resp_sent = 0;
            if (!conn.resp_close_framed and conn.client_keep) {
                // rewrite "Connection: close" out of the head so the client can
                // reuse the connection (Content-Length still frames the body)
                const old_end = conn.resp_head_end;
                conn.resp_head_end = dropHeaderLine(conn.resp[0..conn.resp_end], conn.resp_head_end, "connection");
                conn.resp_end -= (old_end - conn.resp_head_end);
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
    fn relayProgress(self: *Loop, conn: *Conn) void {
        if (conn.resp_close_framed) {
            if (conn.resp_sent < conn.resp_end) {
                self.submitSend(conn, conn.client, conn.resp[conn.resp_sent..conn.resp_end]);
                return;
            }
            self.submitRecv(conn, conn.backend.?, conn.resp[0..]); // wait for EOF
            return;
        }
        const target: usize = @intCast(conn.resp_head_end + (conn.resp_cl orelse unreachable));
        const have_cl = @min(conn.resp_end, target);
        if (conn.resp_sent < have_cl) {
            self.submitSend(conn, conn.client, conn.resp[conn.resp_sent..have_cl]);
            return;
        }
        if (conn.resp_end < target) {
            self.submitRecv(conn, conn.backend.?, conn.resp[conn.resp_end..]);
            return;
        }
        self.nextRequest(conn); // full response delivered
    }

    fn nextRequest(self: *Loop, conn: *Conn) void {
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

/// Removes a single case-insensitive header line (e.g. "Connection: close")
/// from the buffered head, shifting the tail left. Returns the new head length.
fn dropHeaderLine(head: []u8, head_end: usize, name: []const u8) usize {
    const first = std.mem.indexOf(u8, head[0..head_end], "\r\n") orelse return head_end;
    var line_start = first + 2; // skip the status line
    while (line_start < head_end) {
        const eol = std.mem.indexOfPos(u8, head[0..head_end], line_start, "\r\n") orelse return head_end;
        const line = head[line_start..eol];
        if (line.len > 0) {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse return head_end;
            if (std.ascii.eqlIgnoreCase(line[0..colon], name)) {
                const removed = (eol + 2) - line_start;
                std.mem.copyForwards(u8, head[line_start..], head[eol + 2 .. head_end]);
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

/// Opens a fresh non-blocking socket already making a connect() against the
/// backend; EINPROGRESS is fine — the io_uring connect follows immediately.
fn sysConnect(sa: *const posix.sockaddr, len: posix.socklen_t) !posix.fd_t {
    const domain: u32 = if (len == @sizeOf(posix.sockaddr.in)) posix.AF.INET else posix.AF.INET6;
    const fd = try sysSocket(domain, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    errdefer sysClose(fd);
    enableTcpNoDelay(fd);
    const rc = linux.connect(fd, sa, len);
    if (rc != 0) {
        const err = posix.errno(rc);
        if (err != .INPROGRESS) return posix.unexpectedErrno(err);
    }
    return fd;
}

pub fn run(allocator: std.mem.Allocator, router: *Router, port: u16, opts: IoUringConfig) !void {
    var sigpipe_action: posix.Sigaction = .{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.PIPE, &sigpipe_action, null);

    var ring = try linux.IoUring.init(opts.entries, 0);
    defer ring.deinit();

    const listen_fd = try sysSocket(posix.AF.INET6, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    defer sysClose(listen_fd);
    try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try posix.setsockopt(listen_fd, posix.IPPROTO.IPV6, posix.IPV6.V6ONLY, &std.mem.toBytes(@as(c_int, 0)));

    const bind_addr: posix.sockaddr.in6 = .{
        .family = posix.AF.INET6,
        .port = std.mem.nativeToBig(u16, port),
        .flowinfo = 0,
        .addr = [_]u8{0} ** 16,
        .scope_id = 0,
    };
    try check(linux.bind(listen_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in6)));
    try check(linux.listen(listen_fd, 128));

    var conns = try ConnPool.init(allocator, 1024);
    defer conns.deinit(allocator);

    var loop = Loop{ .ring = ring, .router = router, .listen_fd = listen_fd, .conns = conns, .opts = opts };
    loop.submitAccept();
    _ = try loop.ring.submit();

    var cqes: [256]linux.io_uring_cqe = undefined;
    while (true) {
        const n = loop.ring.copy_cqes(&cqes, 1) catch |err| {
            log.fmt("copy_cqes failed: {any}", .{err});
            continue;
        };
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == acceptable_magic) {
                loop.acceptReady(cqe.res);
            } else {
                loop.onOp(@ptrFromInt(cqe.user_data), cqe.res);
            }
        }
        _ = loop.ring.submit() catch {};
    }
}