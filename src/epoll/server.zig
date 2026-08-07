//! epoll-based HTTP/1.1 proxy loops, framed for keep-alive.
//!
//! By default one event loop; with `EpollConfig.loops > 1` there are N loops,
//! each with its own SO_REUSEPORT listener, epoll fd, and partitioned ConnPool
//! + backend pool, so the CPU-bound keep-alive path scales across cores. Each
//! loop is: level-triggered epoll, non-blocking sockets everywhere.
//! Each client connection is a state machine; the backend socket is borrowed
//! from (and returned to) a shared pool so it is reused across requests —
//! including across different clients. Only ONE fd per connection has non-zero
//! epoll interest at a time, so events dispatch on `state`.
//!
//! Keep-alive on both hops, mirroring the threaded backend:
//!   - The request goes out with "Connection: keep-alive" when the client
//!     asked and opts.backend_keepalive lets us reuse the socket.
//!   - The response head is parsed for framing (Content-Length, chunked, or
//!     none → bodyless HEAD/1xx/204/304). Framed responses are relayed to an
//!     exact boundary so both hops can keep alive; a close-framed response is
//!     relayed to EOF and never reused.
//!   - A pooled backend is only re-used for cleanly-framed responses with no
//!     "Connection: close". A socket the backend idle-closed while pooled is
//!     detected on the first write and reconnected once (like threaded).
//!
//! Request bodies: Content-Length is streamed at any size; chunked bodies stream
//! verbatim through http/chunked.zig's framing scanner. The request read window
//! doubles for body streaming; bytes beyond a consumed request (a pipelined
//! next request) are kept in the window and reseed the next keep-alive one.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const net = std.Io.net;

const http = @import("../http/parser.zig");
const Parser = http.Parser;
const Request = http.Request;
const Chunked = @import("../http/chunked.zig");
const util = @import("../http/util.zig");

const Router = @import("../router.zig").Router;
const EpollConfig = @import("../config.zig").EpollConfig;
const log = @import("../log.zig");

const buf_size = 8192;
const max_events = 1024;

const State = enum {
    read_head, // client IN: parse a request head
    write_req, // backend OUT: write head, then stream the request body
    read_cl_body, // client IN: read more Content-Length body
    read_chunk_body, // client IN: read more chunked body
    resp_head, // backend IN: accumulate + parse response head
    resp_cl, // backend IN: relay Content-Length body
    resp_chunk, // backend IN: relay chunked body
    resp_close, // backend IN: relay close-framed body to EOF
    flush_client, // client OUT: drain w, then next
    resp_end, // settled bookkeeping after a fully-sent response
};

const BodyKind = enum { none, content_length, chunked };

const Conn = struct {
    client: posix.fd_t,
    backend: ?posix.fd_t = null,
    backend_addr: net.IpAddress = undefined,
    pooled_socket: bool = false, // backend came from the pool (stale-retry allowed)
    stale_retried: bool = false,
    peer: posix.sockaddr.in6 = undefined,

    state: State = .read_head,
    next: State = .resp_cl, // target once a flush_client drain finishes
    opts: EpollConfig,

    // Client request read window: head, then body, then any pipelined tail.
    inbox: [buf_size]u8 = undefined,
    in_start: usize = 0, // first byte not yet forwarded/scanned (body)
    in_end: usize = 0,
    parser: Parser = .{},
    req: ?Request = null,
    body_kind: BodyKind = .none,
    body_left: u64 = 0, // Content-Length bytes not yet forwarded
    chunk_scan: Chunked.Scanner = .{}, // chunked request framing
    chunk_done: bool = false,

    // Rebuilt request head sent to the backend.
    head: [buf_size]u8 = undefined,
    head_end: usize = 0,
    head_sent: usize = 0,
    // Accepted chunked request bytes, flushed to the backend.
    body_buf: [buf_size]u8 = undefined,
    body_buf_len: usize = 0,
    body_buf_sent: usize = 0,

    // Response head (accumulated, then reconstructed toward the client).
    resp_h: [buf_size]u8 = undefined,
    resp_h_end: usize = 0,
    resp_content_length: ?u64 = null,
    resp_chunked: bool = false,
    resp_conn_close: bool = false,
    resp_bodyless: bool = false,
    resp_scan: Chunked.Scanner = .{},
    resp_body_left: u64 = 0,
    // Body bytes read past the response-head terminator in the same socket
    // read; drained by the first relay call before reading the socket again.
    resp_pend: [buf_size]u8 = undefined,
    resp_pend_start: usize = 0,
    resp_pend_end: usize = 0,

    // Client-bound buffer: reconstructed response head, then response body.
    w: [buf_size]u8 = undefined,
    w_start: usize = 0,
    w_end: usize = 0,

    client_keep: bool = false, // keep the client connection alive
    resp_reusable: bool = false, // return this backend socket to the pool
};

/// Fixed pool of preallocated Conn slots recycled through a free list. The
/// single-threaded loop never needs the allocator in the hot path — a Conn is
/// acquired/released by index, not created/freed — and memory stays bounded by
/// `max_connections` no matter the load or leak. Accept is refused when empty.
const ConnPool = struct {
    slots: []Conn,
    free: []usize,
    top: usize = 0, // count of free slots (free[0..top])

    fn init(allocator: std.mem.Allocator, n: usize) !ConnPool {
        const slots = try allocator.alloc(Conn, n);
        const free = try allocator.alloc(usize, n);
        var pool = ConnPool{ .slots = slots, .free = free };
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

const Pool = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayListUnmanaged(Item) = .empty,
    cap: usize,

    const Item = struct { fd: posix.fd_t, addr: net.IpAddress };

    fn init(allocator: std.mem.Allocator, cap: usize) !Pool {
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
    allocator: std.mem.Allocator,
    router: *Router,
    epfd: posix.fd_t,
    pool: Pool,
    conns: ConnPool,

    // ---- accept / dispatch ----

    fn acceptClients(self: *Loop, listen_fd: posix.fd_t, opts: EpollConfig) void {
        while (true) {
            var peer: posix.sockaddr.in6 = undefined;
            var peer_len: posix.socklen_t = @sizeOf(posix.sockaddr.in6);
            const fd = sysAccept4(listen_fd, @ptrCast(&peer), &peer_len, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK) catch |err| {
                if (err == error.WouldBlock) return;
                log.fmt("accept failed: {any}", .{err});
                return;
            };
            enableTcpNoDelay(fd);
            const conn = self.conns.acquire() orelse {
                sysClose(fd); // at max_connections: refuse rather than queue
                continue;
            };
            conn.* = .{ .client = fd, .peer = peer, .opts = opts };
            self.addWatch(fd, linux.EPOLL.IN, conn) catch |err| {
                log.fmt("epoll add failed: {any}", .{err});
                sysClose(fd);
                self.conns.release(conn);
            };
        }
    }

    fn onEvent(self: *Loop, conn: *Conn) void {
        switch (conn.state) {
            .read_head => self.readHead(conn),
            .write_req => self.pumpRequest(conn),
            .read_cl_body => self.readClBody(conn),
            .read_chunk_body => self.readChunkBody(conn),
            .resp_head => self.readRespHead(conn),
            .resp_cl => self.relayCl(conn),
            .resp_chunk => self.relayChunked(conn),
            .resp_close => self.relayClose(conn),
            .flush_client => self.flushClient(conn),
            .resp_end => {},
        }
    }

    /// Re-registers epoll interest for the current state. The inactive side is
    /// zeroed so only one fd per connection can ever fire.
    fn enterState(self: *Loop, conn: *Conn) void {
        const client_events: u32 = switch (conn.state) {
            .read_head, .read_cl_body, .read_chunk_body => linux.EPOLL.IN,
            .flush_client => linux.EPOLL.OUT,
            else => 0,
        };
        const backend_events: u32 = switch (conn.state) {
            .write_req => linux.EPOLL.OUT,
            .resp_head, .resp_cl, .resp_chunk, .resp_close => linux.EPOLL.IN,
            else => 0,
        };
        self.modWatch(conn.client, client_events, conn) catch {};
        if (conn.backend) |fd| self.modWatch(fd, backend_events, conn) catch {};
    }

    // ---- request: read head, forward ----

    fn readHead(self: *Loop, conn: *Conn) void {
        while (true) {
            if (conn.in_start > 0) {
                // Compact the leftover (pipelined tail / reseed) to the front.
                const left = conn.in_end - conn.in_start;
                std.mem.copyForwards(u8, conn.inbox[0..left], conn.inbox[conn.in_start..conn.in_end]);
                conn.in_start = 0;
                conn.in_end = left;
            }
            if (conn.parser.next(conn.inbox[0..conn.in_end]) catch {
                sendSimple(conn, "400 Bad Request");
                self.teardown(conn);
                return;
            }) |req| {
                // Body begins where the head ends; in_start points past it.
                conn.in_start = req.consumed;
                self.startRequest(conn, req);
                return;
            }
            if (conn.in_end == conn.inbox.len) {
                sendSimple(conn, "431 Request Header Fields Too Large");
                self.teardown(conn);
                return;
            }
            const n = posix.read(conn.client, conn.inbox[conn.in_end..]) catch |err| {
                if (err == error.WouldBlock) return;
                self.teardown(conn);
                return;
            };
            if (n == 0) {
                self.teardown(conn);
                return;
            }
            conn.in_end += n;
        }
    }

    fn startRequest(self: *Loop, conn: *Conn, req: Request) void {
        conn.req = req;
        if (req.chunked and !conn.opts.chunked_requests) {
            sendSimple(conn, "415 Unsupported Media Type");
            self.teardown(conn);
            return;
        }
        conn.body_kind = if (req.chunked) .chunked else if (req.content_length != null) .content_length else .none;
        conn.body_left = req.content_length orelse 0;
        conn.chunk_scan = .{};
        conn.chunk_done = false;
        conn.resp_h_end = 0;

        self.buildHead(conn, req) catch {
            self.teardown(conn); // rebuilt head overran its buffer
            return;
        };

        const host_header = req.header("host") orelse {
            sendSimple(conn, "400 Bad Request");
            self.teardown(conn);
            return;
        };
        const bal = self.router.route(util.hostOnly(host_header)) orelse {
            self.teardown(conn); // nginx `return 444`-style: deny by closing
            return;
        };
        conn.backend_addr = bal.pick();

        if (conn.opts.backend_keepalive) {
            if (self.pool.borrow(conn.backend_addr)) |fd| {
                conn.backend = fd;
                conn.pooled_socket = true;
                conn.state = .write_req;
                if (self.addWatch(fd, linux.EPOLL.OUT, conn) catch null) |_| {
                    self.enterState(conn);
                    self.pumpRequest(conn);
                    return;
                }
                // Watch died on the pooled socket — treat as lost and fall
                // through to a fresh connect.
                conn.backend = null;
                conn.pooled_socket = false;
            }
        }
        conn.pooled_socket = false;
        conn.stale_retried = false;
        conn.backend = connectStart(conn.backend_addr) catch |err| {
            log.fmt("connect failed: {any}", .{err});
            self.teardown(conn);
            return;
        };
        conn.state = .write_req;
        self.addWatch(conn.backend.?, 0, conn) catch {
            self.teardown(conn);
            return;
        };
        self.enterState(conn);
        self.pumpRequest(conn);
    }

    fn buildHead(self: *Loop, conn: *Conn, req: Request) !void {
        _ = self;
        var h: usize = 0;
        h += (try std.fmt.bufPrint(conn.head[h..], "{s} {s} HTTP/1.1\r\n", .{ req.method, req.target })).len;
        for (req.headers) |hd| {
            if (std.ascii.eqlIgnoreCase(hd.name, "x-forwarded-for")) continue;
            h += (try std.fmt.bufPrint(conn.head[h..], "{s}: {s}\r\n", .{ hd.name, hd.value })).len;
        }
        h = try util.writeForwardedFor(&conn.head, h, conn.peer.addr);
        h += (try std.fmt.bufPrint(conn.head[h..], "Via: 1.1 reverse-proxy\r\n", .{})).len;
        if (req.chunked) {
            h += (try std.fmt.bufPrint(conn.head[h..], "Transfer-Encoding: chunked\r\n", .{})).len;
        }
        const keep_backend = req.keep_alive and conn.opts.backend_keepalive;
        h += (try std.fmt.bufPrint(conn.head[h..], "Connection: {s}\r\n\r\n", .{if (keep_backend) "keep-alive" else "close"})).len;
        conn.head_end = h;
        conn.head_sent = 0;
        conn.body_buf_len = 0;
        conn.body_buf_sent = 0;
    }

    fn pumpRequest(self: *Loop, conn: *Conn) void {
        if (conn.head_sent < conn.head_end) {
            const w = sysWrite(conn.backend.?, conn.head[conn.head_sent..conn.head_end]) catch |err| {
                if (err == error.WouldBlock) return;
                self.handleWriteError(conn);
                return;
            };
            conn.head_sent += w;
            if (conn.head_sent < conn.head_end) return;
        }
        self.pumpBody(conn);
    }

    /// Reclaims the read window when it's fully drained (so body streaming can
    /// reuse it), but never if a pipelined tail is still buffered.
    fn reclaimInbox(conn: *Conn) void {
        if (conn.in_start == conn.in_end) {
            conn.in_start = 0;
            conn.in_end = 0;
        }
    }

    fn pumpBody(self: *Loop, conn: *Conn) void {
        switch (conn.body_kind) {
            .none => self.gotoResponse(conn),
            .content_length => {
                const backend = conn.backend.?;
                // Forward already-buffered body bytes (bounded by body_left so
                // a pipelined tail beyond Content-Length is never sent).
                while (conn.in_start < conn.in_end and conn.body_left > 0) {
                    const available = conn.in_end - conn.in_start;
                    const want = @min(@as(usize, @intCast(conn.body_left)), available);
                    const w = sysWrite(backend, conn.inbox[conn.in_start .. conn.in_start + want]) catch |err| {
                        if (err == error.WouldBlock) return;
                        self.teardown(conn);
                        return;
                    };
                    conn.in_start += w;
                    conn.body_left -= w;
                    if (conn.body_left > 0 and w < want) return; // backend blocked
                }
                reclaimInbox(conn);
                if (conn.body_left > 0) {
                    conn.state = .read_cl_body;
                    self.enterState(conn);
                } else {
                    self.gotoResponse(conn);
                }
            },
            .chunked => {
                while (true) {
                    const backend = conn.backend.?;
                    while (conn.body_buf_sent < conn.body_buf_len) {
                        const w = sysWrite(backend, conn.body_buf[conn.body_buf_sent..conn.body_buf_len]) catch |err| {
                            if (err == error.WouldBlock) return;
                            self.teardown(conn);
                            return;
                        };
                        conn.body_buf_sent += w;
                        if (conn.body_buf_sent < conn.body_buf_len) return;
                    }
                    conn.body_buf_len = 0;
                    conn.body_buf_sent = 0;
                    if (conn.chunk_done) {
                        self.gotoResponse(conn);
                        return;
                    }
                    if (conn.in_start == conn.in_end) {
                        reclaimInbox(conn);
                        conn.state = .read_chunk_body;
                        self.enterState(conn);
                        return;
                    }
                    self.consumeChunked(conn);
                }
            },
        }
    }

/// Feeds buffered raw chunked bytes through the scanner, copying the
    /// accepted (byte-for-byte) stream into body_buf for forwarding.
    fn consumeChunked(self: *Loop, conn: *Conn) void {
        const available = conn.in_end - conn.in_start;
        const res = conn.chunk_scan.feed(conn.inbox[conn.in_start..conn.in_end]) catch {
            sendSimple(conn, "400 Bad Request");
            self.teardown(conn);
            return;
        };
        const accepted = @min(@as(usize, res.consumed), available);
        std.mem.copyForwards(u8, conn.body_buf[0..accepted], conn.inbox[conn.in_start .. conn.in_start + accepted]);
        conn.in_start += accepted;
        conn.body_buf_len = accepted;
        conn.body_buf_sent = 0;
        conn.chunk_done = res.done;
    }

    fn readClBody(self: *Loop, conn: *Conn) void {
        const n = posix.read(conn.client, conn.inbox[conn.in_end..]) catch |err| {
            if (err == error.WouldBlock) return;
            self.teardown(conn);
            return;
        };
        if (n == 0) {
            self.teardown(conn);
            return;
        }
        conn.in_end += n;
        conn.state = .write_req;
        self.enterState(conn);
        self.pumpRequest(conn);
    }

    fn readChunkBody(self: *Loop, conn: *Conn) void {
        const n = posix.read(conn.client, conn.inbox[conn.in_end..]) catch |err| {
            if (err == error.WouldBlock) return;
            self.teardown(conn);
            return;
        };
        if (n == 0) {
            self.teardown(conn);
            return;
        }
        conn.in_end += n;
        conn.state = .write_req;
        self.enterState(conn);
        self.pumpRequest(conn);
    }

    /// Stale pooled socket: the backend closed it while idle. Reconnect and
    /// re-send only when nothing of the request has gone out yet (the usual
    /// idle-close case, and the only one where re-sending is well-defined).
    fn handleWriteError(self: *Loop, conn: *Conn) void {
        if (conn.pooled_socket and !conn.stale_retried and conn.head_sent == 0) {
            conn.stale_retried = true;
            self.delWatch(conn.backend.?);
            sysClose(conn.backend.?);
            conn.backend = connectStart(conn.backend_addr) catch {
                self.teardown(conn);
                return;
            };
            conn.pooled_socket = false;
            conn.head_sent = 0;
            self.addWatch(conn.backend.?, 0, conn) catch {
                self.teardown(conn);
                return;
            };
            self.enterState(conn);
            self.pumpRequest(conn);
            return;
        }
        log.fmt("backend write failed (seq); closing", .{});
        self.teardown(conn);
    }

    // ---- response relay ----

    fn gotoResponse(self: *Loop, conn: *Conn) void {
        conn.state = .resp_head;
        self.enterState(conn);
        self.readRespHead(conn);
    }

    fn readRespHead(self: *Loop, conn: *Conn) void {
        while (true) {
            if (std.mem.indexOf(u8, conn.resp_h[0..conn.resp_h_end], "\r\n\r\n")) |term| {
                self.startRespBody(conn, term);
                return;
            }
            if (conn.resp_h_end >= conn.resp_h.len) {
                self.teardown(conn);
                return;
            }
            const n = posix.read(conn.backend.?, conn.resp_h[conn.resp_h_end..]) catch |err| {
                if (err == error.WouldBlock) return;
                self.teardown(conn);
                return;
            };
            if (n == 0) {
                self.teardown(conn);
                return;
            }
            conn.resp_h_end += n;
        }
    }

    /// Parses the response head, reconstructs it for the client hop, and picks
    /// the body relay (and whether either hop survives the response).
    fn startRespBody(self: *Loop, conn: *Conn, term: usize) void {
        const head = conn.resp_h[0..term];
        var content_length: ?u64 = null;
        var chunked = false;
        var conn_close = false;
        conn.resp_scan = .{};

        conn.w_start = 0;
        conn.w_end = 0;
        var lines = std.mem.splitSequence(u8, head, "\r\n");
        const status_line = lines.next() orelse {
            self.teardown(conn);
            return;
        };
        self.wAppend(conn, status_line);
        self.wAppend(conn, "\r\n");
        while (lines.next()) |line| {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const name = std.mem.trim(u8, line[0..colon], " \t");
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
            if (std.ascii.eqlIgnoreCase(name, "content-length")) {
                content_length = std.fmt.parseUnsigned(u64, value, 10) catch {
                    self.teardown(conn);
                    return;
                };
            } else if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
                if (util.containsTokenIgnoreCase(value, "chunked")) chunked = true;
            } else if (std.ascii.eqlIgnoreCase(name, "connection")) {
                if (util.containsTokenIgnoreCase(value, "close")) conn_close = true;
            }
            if (std.ascii.eqlIgnoreCase(name, "connection") or std.ascii.eqlIgnoreCase(name, "keep-alive")) continue;
            self.wAppend(conn, line);
            self.wAppend(conn, "\r\n");
        }

        const is_head = std.ascii.eqlIgnoreCase(conn.req.?.method, "HEAD");
        const bodyless = is_head or util.statusHasNoBody(status_line);
        const has_framing = content_length != null or chunked;

        conn.resp_content_length = content_length;
        conn.resp_chunked = chunked;
        conn.resp_conn_close = conn_close;
        conn.resp_bodyless = bodyless;
        conn.client_keep = conn.req.?.keep_alive and conn.opts.client_keepalive and has_framing;
        conn.resp_reusable = conn.opts.backend_keepalive and (has_framing or bodyless) and !conn_close;
        conn.resp_body_left = content_length orelse 0;
        conn.resp_scan = .{};
        conn.resp_pend_start = 0;
        conn.resp_pend_end = 0;

        // Any body bytes already read past the head terminator stay buffered
        // (single-segment response) and are drained by the relay before it
        // touches the socket again.
        const tail = head.len + 4;
        if (conn.resp_h_end > tail) {
            const n = conn.resp_h_end - tail;
            std.mem.copyForwards(u8, conn.resp_pend[0..n], conn.resp_h[tail..conn.resp_h_end]);
            conn.resp_pend_end = n;
        }

        self.wAppend(conn, "Connection: ");
        self.wAppend(conn, if (conn.client_keep) "keep-alive" else "close");
        self.wAppend(conn, "\r\n\r\n");

        const body_mode: State = if (bodyless)
            .resp_end
        else if (chunked)
            .resp_chunk
        else if (content_length != null)
            .resp_cl
        else
            .resp_close;

        conn.state = .flush_client;
        conn.next = body_mode;
        self.enterState(conn);
        self.flushClient(conn);
    }

    fn wAppend(self: *Loop, conn: *Conn, bytes: []const u8) void {
        _ = self;
        if (conn.w_end + bytes.len <= conn.w.len) {
            std.mem.copyForwards(u8, conn.w[conn.w_end..], bytes);
            conn.w_end += bytes.len;
        }
        // else: response too large to fit the buffer — drop (teardown path).
    }

    fn flushClient(self: *Loop, conn: *Conn) void {
        while (conn.w_start < conn.w_end) {
            const n = sysWrite(conn.client, conn.w[conn.w_start..conn.w_end]) catch |err| {
                if (err == error.WouldBlock) return; // stay in flush_client
                self.teardown(conn);
                return;
            };
            conn.w_start += n;
        }
        conn.w_start = 0;
        conn.w_end = 0;
        // Drained — continue with the target state.
        switch (conn.next) {
            .resp_end => self.doRespEnd(conn),
            .resp_cl => {
                conn.state = .resp_cl;
                self.enterState(conn);
                self.relayCl(conn);
            },
            .resp_chunk => {
                conn.state = .resp_chunk;
                self.enterState(conn);
                self.relayChunked(conn);
            },
            .resp_close => {
                conn.state = .resp_close;
                self.enterState(conn);
                self.relayClose(conn);
            },
            else => unreachable,
        }
    }

    /// Reads response body bytes that were buffered past the head terminator,
    /// before falling back to the socket. Returns 0 only when empty.
    fn pendRead(self: *Loop, conn: *Conn, dst: []u8) usize {
        _ = self;
        const n = @min(conn.resp_pend_end - conn.resp_pend_start, dst.len);
        if (n > 0)
            std.mem.copyForwards(u8, dst[0..n], conn.resp_pend[conn.resp_pend_start .. conn.resp_pend_start + n]);
        conn.resp_pend_start += n;
        if (conn.resp_pend_start == conn.resp_pend_end) {
            conn.resp_pend_start = 0;
            conn.resp_pend_end = 0;
        }
        return n;
    }

    fn relayCl(self: *Loop, conn: *Conn) void {
        if (conn.resp_body_left == 0) {
            conn.state = .flush_client;
            conn.next = .resp_end;
            self.enterState(conn);
            self.flushClient(conn);
            return;
        }
        const want = @min(@as(u64, buf_size), conn.resp_body_left);
        const room = conn.w[0..@intCast(want)];
        var n: usize = 0;
        if (conn.resp_pend_start < conn.resp_pend_end) {
            n = self.pendRead(conn, room);
        } else {
            const r = posix.read(conn.backend.?, room) catch |err| switch (err) {
                error.WouldBlock => return,
                else => {
                    self.teardown(conn);
                    return;
                },
            };
            if (r == 0) {
                conn.resp_reusable = false; // backend closed mid-body
                conn.resp_body_left = 0;
                conn.state = .flush_client;
                conn.next = .resp_end;
                self.enterState(conn);
                self.flushClient(conn);
                return;
            }
            n = @intCast(r);
        }
        if (n == 0) return; // nothing staged yet, still pending
        conn.w_end = n;
        conn.resp_body_left -= n;
        conn.state = .flush_client;
        conn.next = if (conn.resp_body_left == 0) .resp_end else .resp_cl;
        self.enterState(conn);
        self.flushClient(conn);
    }

    fn relayChunked(self: *Loop, conn: *Conn) void {
        var n: usize = 0;
        if (conn.resp_pend_start < conn.resp_pend_end) {
            n = self.pendRead(conn, conn.w[0..]);
        } else {
            const r = posix.read(conn.backend.?, conn.w[0..]) catch |err| {
                if (err == error.WouldBlock) return;
                self.teardown(conn);
                return;
            };
            if (r == 0) {
                conn.resp_reusable = false;
                conn.state = .flush_client;
                conn.next = .resp_end;
                self.enterState(conn);
                self.flushClient(conn);
                return;
            }
            n = @intCast(r);
        }
        if (n == 0) return;
        const res = conn.resp_scan.feed(conn.w[0..n]) catch {
            self.teardown(conn);
            return;
        };
        conn.w_end = @min(@as(usize, res.consumed), n);
        conn.state = .flush_client;
        conn.next = if (res.done) .resp_end else .resp_chunk;
        self.enterState(conn);
        self.flushClient(conn);
    }

    fn relayClose(self: *Loop, conn: *Conn) void {
        var n: usize = 0;
        if (conn.resp_pend_start < conn.resp_pend_end) {
            n = self.pendRead(conn, conn.w[0..]);
        } else {
            const r = posix.read(conn.backend.?, conn.w[0..]) catch |err| {
                if (err == error.WouldBlock) return;
                self.teardown(conn);
                return;
            };
            if (r == 0) {
                conn.state = .flush_client;
                conn.next = .resp_end;
                self.enterState(conn);
                self.flushClient(conn);
                return;
            }
            n = @intCast(r);
        }
        if (n == 0) return;
        conn.w_end = n;
        conn.state = .flush_client;
        conn.next = .resp_close;
        self.enterState(conn);
        self.flushClient(conn);
    }

    fn doRespEnd(self: *Loop, conn: *Conn) void {
        // Backend socket: pool it if the response was cleanly framed for reuse.
        if (conn.backend) |fd| {
            self.delWatch(fd);
            if (conn.resp_reusable) {
                self.pool.release(fd, conn.backend_addr);
            } else {
                sysClose(fd);
            }
            conn.backend = null;
        }
        if (conn.client_keep) {
            // keep-alive: the read window (pipelined tail) reseeds the next
            // request; try parsing leftover bytes already in hand.
            conn.state = .read_head;
            self.enterState(conn);
            self.readHead(conn);
        } else {
            self.teardown(conn);
        }
    }

    fn sendSimple(conn: *Conn, status: []const u8) void {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "HTTP/1.1 {s}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            .{status},
        ) catch return;
        _ = sysWrite(conn.client, msg) catch {};
    }

    fn teardown(self: *Loop, conn: *Conn) void {
        if (conn.backend) |fd| {
            self.delWatch(fd);
            sysClose(fd);
        }
        self.delWatch(conn.client);
        sysClose(conn.client);
        self.conns.release(conn);
    }

    fn addWatch(self: *Loop, fd: posix.fd_t, events: u32, conn: *Conn) !void {
        var ev: linux.epoll_event = .{ .events = events, .data = .{ .ptr = @intFromPtr(conn) } };
        try check(linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_ADD, fd, &ev));
    }

    fn modWatch(self: *Loop, fd: posix.fd_t, events: u32, conn: *Conn) !void {
        var ev: linux.epoll_event = .{ .events = events, .data = .{ .ptr = @intFromPtr(conn) } };
        try check(linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_MOD, fd, &ev));
    }

    fn delWatch(self: *Loop, fd: posix.fd_t) void {
        _ = linux.epoll_ctl(self.epfd, linux.EPOLL.CTL_DEL, fd, null);
    }
};

pub fn run(allocator: std.mem.Allocator, router: *Router, port: u16, opts: EpollConfig) !void {
    var sigpipe_action: posix.Sigaction = .{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.PIPE, &sigpipe_action, null);

    // N independent event loops, each with its own SO_REUSEPORT listener so the
    // kernel hash-balances accepted connections across them. Max_conections and
    // the backend pool are partitioned per loop (lock-free; ~even under kernel
    // hashing). The calling thread runs the last loop so run() blocks forever,
    // like threaded.
    const n = @max(@as(usize, 1), opts.loops);
    for (0..n - 1) |_| {
        _ = std.Thread.spawn(.{}, loopWorker, .{ allocator, router, port, opts }) catch |err| {
            log.fmt("epoll loop spawn failed: {any}", .{err});
        };
    }
    loopWorker(allocator, router, port, opts);
}

fn loopWorker(allocator: std.mem.Allocator, router: *Router, port: u16, opts: EpollConfig) void {
    const n = @max(@as(usize, 1), opts.loops);

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
    if (linux.bind(listen_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in6)) != 0) return;
    if (linux.listen(listen_fd, 128) != 0) return;

    const epfd = epollCreate() catch |err| {
        log.fmt("epoll_create1 failed: {any}", .{err});
        return;
    };
    defer sysClose(epfd);

    var lev: linux.epoll_event = .{ .events = linux.EPOLL.IN, .data = .{ .fd = listen_fd } };
    if (linux.epoll_ctl(epfd, linux.EPOLL.CTL_ADD, listen_fd, &lev) != 0) return;

    // Total max_connections budget split across loops; each loop refuses accepts
    // when its own share is exhausted. Remainder is dropped (per = floor(n)).
    const conns = ConnPool.init(allocator, @max(@as(usize, 1), opts.max_connections / n)) catch return;
    defer conns.deinit(allocator);

    // Backend pool cap also partitioned so total idle sockets stay ~bounded.
    const pool = Pool.init(allocator, @max(@as(usize, 1), opts.pool_size / n)) catch return;
    defer pool.deinit();

    var loop = Loop{
        .allocator = allocator,
        .router = router,
        .epfd = epfd,
        .conns = conns,
        .pool = pool,
    };

    var events: [max_events]linux.epoll_event = undefined;
    while (true) {
        const nr = epollWait(epfd, &events, @intCast(events.len), -1) catch |err| {
            log.fmt("epoll_wait failed: {any}", .{err});
            continue;
        };
        for (events[0..nr]) |ev| {
            if (ev.data.fd == listen_fd) {
                loop.acceptClients(listen_fd, opts);
                continue;
            }
            const conn: *Conn = @ptrFromInt(ev.data.ptr);
            loop.onEvent(conn);
        }
    }
}

// ---- raw syscall helpers (see threaded/server.zig's header for the rationale) ----

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

fn sysAccept4(fd: posix.fd_t, addr: *posix.sockaddr, addr_len: *posix.socklen_t, flags: u32) !posix.fd_t {
    const rc = linux.accept4(fd, addr, addr_len, flags);
    try check(rc);
    return @intCast(rc);
}

fn sysClose(fd: posix.fd_t) void {
    _ = linux.close(fd);
}

fn sysWrite(fd: posix.fd_t, buf: []const u8) !usize {
    const rc = linux.write(fd, buf.ptr, buf.len);
    try check(rc);
    return rc;
}

fn enableTcpNoDelay(fd: posix.fd_t) void {
    posix.setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};
}

fn epollCreate() !posix.fd_t {
    const rc = linux.epoll_create1(0);
    try check(rc);
    return @intCast(rc);
}

fn epollWait(epfd: posix.fd_t, events: [*]linux.epoll_event, maxevents: u32, timeout: i32) !usize {
    while (true) {
        const rc = linux.epoll_wait(epfd, events, maxevents, timeout);
        return switch (posix.errno(rc)) {
            .SUCCESS => rc,
            .INTR => continue,
            else => |e| posix.unexpectedErrno(e),
        };
    }
}

fn connectStart(addr: net.IpAddress) !posix.fd_t {
    switch (addr) {
        .ip4 => |ip4| {
            const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
            errdefer sysClose(fd);
            enableTcpNoDelay(fd);
            const sa: posix.sockaddr.in = .{
                .family = posix.AF.INET,
                .port = std.mem.nativeToBig(u16, ip4.port),
                .addr = @bitCast(ip4.bytes),
                .zero = [_]u8{0} ** 8,
            };
            const rc = linux.connect(fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.in));
            if (rc != 0) {
                const err = posix.errno(rc);
                if (err != .INPROGRESS) return posix.unexpectedErrno(err);
            }
            return fd;
        },
        .ip6 => |ip6| {
            const fd = try sysSocket(posix.AF.INET6, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
            errdefer sysClose(fd);
            enableTcpNoDelay(fd);
            const sa: posix.sockaddr.in6 = .{
                .family = posix.AF.INET6,
                .port = std.mem.nativeToBig(u16, ip6.port),
                .flowinfo = 0,
                .addr = ip6.bytes,
                .scope_id = 0,
            };
            const rc = linux.connect(fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.in6));
            if (rc != 0) {
                const err = posix.errno(rc);
                if (err != .INPROGRESS) return posix.unexpectedErrno(err);
            }
            return fd;
        },
    }
}