//! Pooled-thread HTTP/1.1 proxy loop.
//!
//! Socket I/O here is raw std.posix (accept/connect/read/write/close), not
//! std.Io.net. That's a deliberate downgrade, not an oversight: Io.net's
//! Stream Reader/Writer were reproducibly losing writes to a client
//! connection whenever a second socket (the backend) was touched in between
//! reading the request and writing the response — exactly the pattern a
//! proxy needs on every request. std.posix is the mature, boring layer this
//! project's own design doc already committed to for the epoll phase (since
//! Io.Evented isn't implemented in 0.16); this just pulls that fallback
//! forward to the threaded phase too. Io.net.IpAddress is still used as a
//! pure data/parsing type (balancer.zig's DNS/hostname resolution is
//! untouched) — only the actual socket syscalls changed.
//!
//! Concurrency model (thread pool): a fixed set of worker threads each run
//! the accept-and-handle loop on the one shared dual-stack listening socket
//! (linux.accept4 is thread-safe — the kernel hands each wake-up a distinct
//! pending connection). This caps the number of live connections at
//! pool_threads and amortizes thread-creation cost, unlike the earlier
//! spawn-a-thread-per-connection design. No job queue: an idle worker that
//! isn't blocking in accept is busy holding one keep-alive connection open,
//! which is exactly what a keep-alive server wants (a connection stays pinned
//! to the worker that accepted it).
//!
//! Current scope (deliberately narrow — see phase-2 epoll work for the rest):
//!   - Client keep-alive (HTTP/1.1). A connection is kept open across requests
//!     as long as the client asked for it and the backend response has framing
//!     (Content-Length or Transfer-Encoding) we can relay deterministically.
//!     Responses without framing (delimited by connection-close) force the
//!     client connection shut, since we can't know where one ends.
//!   - Content-Length and chunked (Transfer-Encoding) request bodies are both
//!     relayed. Chunked bodies stream through http/chunked.zig's framing
//!     scanner — forwarded verbatim, never decoded or buffered — so a large
//!     upload doesn't cost memory and the body boundary is known for keep-alive.
//!   - One backend connection per *client* connection, reused across that
//!     client's keep-alive requests: we send the backend "Connection:
//!     keep-alive" and only drop the socket when the response has no framing,
//!     the backend sends Connection: close, or it closes early. Backend reuse
//!     is per-client, so no cross-thread pool or locks are needed — the socket
//!     belongs to whichever worker thread owns the client connection. A stale
//!     reused socket (backend idle-closed it) is detected on write and
//!     reconnected once.
//!   - One dual-stack listener (AF_INET6 with IPV6_V6ONLY cleared), so v4
//!     clients arrive here too, mapped as ::ffff:a.b.c.d — no separate v4
//!     accept loop. Backends may be IPv4 or IPv6 (connectTo handles both)
//!     since that comes from balancer.zig's existing resolution.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const net = std.Io.net; // type-only: IpAddress from balancer.zig, no I/O calls

const http = @import("../http/parser.zig");
const Parser = http.Parser;
const Request = http.Request;
const Chunked = @import("../http/chunked.zig");
const util = @import("../http/util.zig");

const RouterMod = @import("../router.zig");
const Router = RouterMod.Router;
const Balancer = @import("../balancer.zig").Balancer;

const max_request_head = 8192;
const max_response_head = 8192;

const ThreadedConfig = @import("../config.zig").ThreadedConfig;
const log = @import("../log.zig");

/// std.posix.{socket,bind,listen,accept,connect} were removed in 0.16
/// (moved into std.Io.net's vtable). We already can't route through
/// std.Io.net.Stream here (see file header — it drops writes), so this
/// drops one layer lower to the raw std.os.linux syscalls std.posix used
/// to wrap, and does its own errno check. read/write/close/setsockopt are
/// untouched — those stayed in std.posix.
fn check(rc: usize) !void {
    return switch (posix.errno(rc)) {
        .SUCCESS => {},

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
    // ponytail: old std.posix.close retried on EINTR and asserted on any
    // other errno. That behavior is gone along with the function; every
    // caller of this here is a best-effort `defer`/`errdefer`, so we just
    // drop the return value like they always ignored close()'s (nonexistent)
    // failure path anyway.
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

pub fn run(allocator: std.mem.Allocator, router: *Router, port: u16, opts: ThreadedConfig) !void {
    // write() to a socket whose peer already closed raises SIGPIPE, and the
    // default action for that is to kill the whole process — not just the
    // thread doing the write. Ignore it so those writes just fail with
    // EPIPE instead, which check() above turns into error.BrokenPipe.
    // Belongs at process startup really (main.zig), not per-listener; it's
    // here because this is the file that owns the raw syscalls now.
    var sigpipe_action: posix.Sigaction = .{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.PIPE, &sigpipe_action, null);

    // Single dual-stack listener instead of separate v4/v6 sockets + accept
    // loops: an AF_INET6 socket with IPV6_V6ONLY cleared accepts both, and
    // v4 peers show up as ::ffff:a.b.c.d (unmapped in writeForwardedFor).
    const listen_fd = try sysSocket(posix.AF.INET6, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    defer sysClose(listen_fd);

    try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    // ponytail: `26` is IPPROTO_IPV6/IPV6_V6ONLY's raw Linux value, used
    // directly because std.os.linux's naming for it has moved around across
    // 0.16 std.Io churn same as everything else in this file. If
    // linux.IPV6.V6ONLY exists in your checkout, prefer that symbol instead.
    try posix.setsockopt(listen_fd, posix.IPPROTO.IPV6, posix.IPV6.V6ONLY, &std.mem.toBytes(@as(c_int, 0)));

    const bind_addr: posix.sockaddr.in6 = .{
        .family = posix.AF.INET6,
        .port = std.mem.nativeToBig(u16, port),
        .flowinfo = 0,
        .addr = [_]u8{0} ** 16, // in6addr_any (covers v4-mapped too)
        .scope_id = 0,
    };
    try check(linux.bind(listen_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in6)));
    try check(linux.listen(listen_fd, 128));

    // Take opts.pool_threads - 1 slots from worker threads; this (main) thread
    // becomes the last worker. Keeps run() blocking forever like the old
    // single accept-loop did, and there's nothing useful for main to do
    // besides pull its share of connections.
    for (0..opts.pool_threads - 1) |_| {
        _ = std.Thread.spawn(.{}, workerLoop, .{ allocator, router, listen_fd, opts }) catch |err| {
            std.debug.print("thread spawn failed: {any}\n", .{err}); // ponytail: not hot path (once per pool), keep direct
        };
    }
    workerLoop(allocator, router, listen_fd, opts);
}

fn workerLoop(allocator: std.mem.Allocator, router: *Router, listen_fd: posix.fd_t, opts: ThreadedConfig) void {
    while (true) {
        var peer: posix.sockaddr.in6 = undefined;
        var peer_len: posix.socklen_t = @sizeOf(posix.sockaddr.in6);
        const conn_fd = sysAccept4(listen_fd, @ptrCast(&peer), &peer_len, posix.SOCK.CLOEXEC) catch |err| {
            log.fmt("accept failed: {any}", .{err});
            continue;
        };
        enableTcpNoDelay(conn_fd);
        handleConnection(allocator, router, conn_fd, peer, opts);
    }
}

fn handleConnection(allocator: std.mem.Allocator, router: *Router, conn_fd: posix.fd_t, peer: posix.sockaddr.in6, opts: ThreadedConfig) void {
    defer sysClose(conn_fd);

    // Keep-alive loop: read one request, forward it, and only close when the
    // client asked to or the response framing says we can't keep the
    // connection unambiguous. The read buffer persists across requests so
    // bytes already pulled off the socket (body leftovers, pipelined heads)
    // aren't lost between iterations.
    //
    // `backend` is the backend connection reused across this client
    // connection's requests. It lives and dies with conn_fd, is touched only
    // by this worker thread, and is connected lazily — so "keep-alive" here is
    // per-client backend reuse (no cross-thread pool, no locks).
    var backend: ?posix.fd_t = null;
    defer if (backend) |fd| sysClose(fd);

    var buf: [max_request_head]u8 = undefined;
    var start: usize = 0;
    var end: usize = 0;

    while (true) {
        var parser: Parser = .{};
        const req: Request = while (true) {
            const got = parser.next(buf[start..end]) catch {
                sendSimple(conn_fd, "400 Bad Request");
                return;
            };
            if (got) |r| break r;
            // else: headers not complete yet — need more data.
            if (end == buf.len) {
                sendSimple(conn_fd, "431 Request Header Fields Too Large");
                return;
            }
            const n = posix.read(conn_fd, buf[end..]) catch |err| {
                log.fmt("read error: {any}", .{err});
                return;
            };
            if (n == 0) return; // peer closed
            end += n;
        };

        start += req.consumed; // skip the request head; leftover is body/next-request

        const host_header = req.header("host") orelse {
            sendSimple(conn_fd, "400 Bad Request");
            return;
        };
        const bal = router.route(util.hostOnly(host_header)) orelse return; // nginx `return 444`-style: deny by closing, no response

        // Chunked request bodies are relayable only when the config enables
        // that path; otherwise reject before touching the backend.
        if (req.chunked and !opts.chunked_requests) {
            sendSimple(conn_fd, "415 Unsupported Media Type");
            return;
        }

        const keep_alive = forward(allocator, bal, conn_fd, peer, req, &buf, &start, &end, &backend, opts) catch |err| {
            log.fmt("proxy error: {any}", .{err});
            return; // any error tears the whole connection down
        };
        // Close after this response when the client asked to, the framing
        // can't be kept unambiguous, or config says never keep the client alive.
        if (!keep_alive or !opts.client_keepalive) return;

        // Slide the unconsumed tail (pipelined next request) to the front for
        // the next iteration.
        const left = end - start;
        std.mem.copyForwards(u8, buf[0..left], buf[start..end]);
        start = 0;
        end = left;
    }
}

fn forward(
    allocator: std.mem.Allocator,
    bal: *Balancer,
    client_fd: posix.fd_t,
    peer: posix.sockaddr.in6,
    req: Request,
    buf: *[max_request_head]u8,
    start: *usize,
    end: *usize,
    backend_opt: *?posix.fd_t,
    opts: ThreadedConfig,
) !bool {
    _ = allocator; // not needed yet — everything here is fixed-size buffers

    // Reuse the existing backend connection if the previous request left it
    // reusable; otherwise open a fresh one. The balancer's preferred address
    // is Picked once per connection and reused throughout its lifetime.
    const backend_addr = bal.pick();
    var backend_fd: posix.fd_t = if (backend_opt.* != null) backend_opt.*.? else try connectTo(backend_addr);
    const was_reused = backend_opt.* != null;
    errdefer sysClose(backend_fd); // any error tears the connection down

    // Build the request head into one buffer, then write it in as few
    // syscalls as possible (writeAll below still loops for partial writes).
    // The backend connection is kept alive and presumed reusable: only a
    // response without framing (or a Connection: close) makes us drop it.
    var head: [max_request_head]u8 = undefined;
    var head_len: usize = 0;
    head_len += (try std.fmt.bufPrint(head[head_len..], "{s} {s} HTTP/1.1\r\n", .{ req.method, req.target })).len;
    for (req.headers) |h| {
        // Drop any client-supplied X-Forwarded-For rather than chaining it —
        // trusting a client-controlled header for the source IP lets a client
        // spoof it. We're the first hop, so we set it ourselves below.
        if (std.ascii.eqlIgnoreCase(h.name, "x-forwarded-for")) continue;
        head_len += (try std.fmt.bufPrint(head[head_len..], "{s}: {s}\r\n", .{ h.name, h.value })).len;
    }
    head_len = try util.writeForwardedFor(&head, head_len, peer.addr);
    head_len += (try std.fmt.bufPrint(head[head_len..], "Via: 1.1 reverse-proxy\r\n", .{})).len;
    // The parser strips Transfer-Encoding as hop-by-hop, but a chunked body
    // must be announced to the backend or its bytes won't make sense to it.
    // Content-Length bodies never carry TE, so the two are mutually exclusive.
    if (req.chunked) {
        head_len += (try std.fmt.bufPrint(head[head_len..], "Transfer-Encoding: chunked\r\n", .{})).len;
    }
    // Only ask the backend to keep the socket alive (and thus pay the reuse
    // bookkeeping) when the client is actually going to reuse this connection
    // AND the config allows backend keep-alive.
    // ponytail: for a Connection: close client every request is one-shot, so
    // requesting keep-alive here adds backend overhead with no reuse payoff —
    // nginx measured 2.5x slower under such a workload than sending close.
    const backend_keepalive = req.keep_alive and opts.backend_keepalive;
    head_len += (try std.fmt.bufPrint(head[head_len..], "Connection: {s}\r\n\r\n", .{if (backend_keepalive) "keep-alive" else "close"})).len;

    // A reused backend may have gone stale while idle (peer closed it, e.g.
    // its own keep-alive timeout fired). A write to a closed-but-buffered
    // socket often doesn't fail until the read below; detect a stale write and
    // reconnect once so a single server-side close doesn't kill the request.
    writeHead: {
        writeAll(backend_fd, head[0..head_len]) catch |err| {
            if (!was_reused) return err;
            sysClose(backend_fd);
            backend_fd = try connectTo(backend_addr);
            try writeAll(backend_fd, head[0..head_len]);
            break :writeHead;
        };
    }

    if (req.chunked) {
        try forwardChunkedBody(client_fd, backend_fd, buf, start, end);
    } else if (req.content_length) |content_length| {
        var remaining = content_length;

        // Consume body bytes that arrived in the buffer already (they follow
        // the head with no gap), then stream the rest straight from the client.
        const bufbod = @min(end.* - start.*, remaining);
        if (bufbod > 0) {
            try writeAll(backend_fd, buf.*[start.* .. start.* + bufbod]);
            start.* += bufbod;
            remaining -= bufbod;
        }

        var pump: [4096]u8 = undefined;
        while (remaining > 0) {
            const want = pump[0..@min(pump.len, remaining)];
            const n = try posix.read(client_fd, want);
            if (n == 0) return error.ClientClosedMidBody;
            try writeAll(backend_fd, want[0..n]);
            remaining -= n;
        }
    }

    const res = try relayResponse(client_fd, backend_fd, req, opts.client_keepalive);
    if (res.backend_keep and backend_keepalive) {
        backend_opt.* = backend_fd; // hand the socket back for the next request
    } else {
        sysClose(backend_fd);
        backend_opt.* = null;
    }
    return res.client_keep;
}

/// Streams a chunked request body to the backend verbatim while scanning for
/// its end, so only the terminal "0\r\n[trailers]\r\n" is consumed — bytes
/// beyond it (a pipelined next request) stay in the [start..end) window for
/// the keep-alive loop.
fn forwardChunkedBody(
    client_fd: posix.fd_t,
    backend_fd: posix.fd_t,
    buf: *[max_request_head]u8,
    start: *usize,
    end: *usize,
) !void {
    var scan: Chunked.Scanner = .{};
    while (!scan.done) {
        if (start.* == end.*) {
            // Buffered body consumed; compact (start sits at end so the whole
            // window is free) and read more from the client.
            start.* = 0;
            end.* = 0;
            const n = try posix.read(client_fd, buf.*[0..]);
            if (n == 0) return error.ClientClosedMidChunked;
            end.* = n;
        }
        const res = try scan.feed(buf.*[start.*..end.*]);
        try writeAll(backend_fd, buf.*[start.* .. start.* + res.consumed]);
        start.* += res.consumed;
    }
}

/// Relays the backend's response to the client, parsing the response head to
/// rewrite the Connection header for the client hop and stream the body to a
/// clean framing boundary so the backend socket can be kept alive and reused.
///
/// Keep-alive (client and backend) is only offered when the response declares
/// framing (Content-Length or Transfer-Encoding) — otherwise the body is
/// delimited by connection-close and there's no unambiguous point to start
/// the next request on the same socket.
const RelayResult = struct {
    client_keep: bool, // keep the client connection alive
    backend_keep: bool, // this backend socket is at a clean message boundary, safe to reuse
};

/// Relays the backend's response to the client. Parses the response head
/// enough to (a) rewrite the Connection header for the client hop,
/// (b) stream the body to an exact framing boundary so the backend socket can
/// be kept alive and reused, and (c) work out both keep-alive verdicts.
///
/// A Backend socket is only returned as reusable when the response had
/// unambiguous framing we consumed in full (Content-Length or chunked). An
/// EOF-framed body, an early close, or a response Connection: close all drop
/// the socket. The client verdict additionally requires explicit framing —
/// a client can't delimit a close-delimited body either.
fn relayResponse(client_fd: posix.fd_t, backend_fd: posix.fd_t, req: Request, client_keepalive: bool) !RelayResult {
    var hbuf: [max_response_head]u8 = undefined;
    var hlen: usize = 0;
    var term: ?usize = null;
    while (term == null) {
        if (hlen >= hbuf.len) return error.ResponseHeadTooLong;
        const n = try posix.read(backend_fd, hbuf[hlen..]);
        if (n == 0) return error.BackendClosedEarly;
        hlen += n;
        term = std.mem.indexOf(u8, hbuf[0..hlen], "\r\n\r\n");
    }
    const t = term.?;
    const head = hbuf[0..t];

    var content_length: ?u64 = null;
    var chunked_te = false;
    var response_conn_close = false;
    var out: [max_response_head]u8 = undefined;
    var out_len: usize = 0;

    var lines = std.mem.splitSequence(u8, head, "\r\n");
    const status_line = lines.next() orelse return error.BadResponseHead;
    out_len += (try std.fmt.bufPrint(out[out_len..], "{s}\r\n", .{status_line})).len;
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        const val = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            content_length = std.fmt.parseUnsigned(u64, val, 10) catch return error.BadResponseHead;
        } else if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
            chunked_te = containsTokenIgnoreCase(val, "chunked");
        } else if (std.ascii.eqlIgnoreCase(name, "connection")) {
            if (containsTokenIgnoreCase(val, "close")) response_conn_close = true;
        }
        // Drop the Connection and Keep-Alive headers; we rewrite Connection
        // for the client hop below. Everything else is relayed verbatim —
        // the client still needs Content-Length / Transfer-Encoding to frame.
        if (std.ascii.eqlIgnoreCase(name, "connection") or std.ascii.eqlIgnoreCase(name, "keep-alive")) continue;
        out_len += (try std.fmt.bufPrint(out[out_len..], "{s}\r\n", .{line})).len;
    }

    const has_framing = content_length != null or chunked_te;
    const client_keep = req.keep_alive and client_keepalive and has_framing;
    out_len += (try std.fmt.bufPrint(
        out[out_len..],
        "Connection: {s}\r\n\r\n",
        .{if (client_keep) "keep-alive" else "close"},
    )).len;
    try writeAll(client_fd, out[0..out_len]);

    // Body boundary for the *backend* socket: messages that carry no body at
    // all (HEAD, 1xx, 204, 304) end right after the head regardless of any
    // Content-Length header, so nothing to drain.
    const bodyless = std.ascii.eqlIgnoreCase(req.method, "head") or
        statusHasNoBody(status_line);
    var backend_keep: bool = undefined;

    if (bodyless) {
        backend_keep = true;
    } else if (chunked_te) {
        backend_keep = try relayChunkedResponseBody(client_fd, backend_fd, hbuf[t + 4 .. hlen]);
    } else if (content_length) |cl| {
        backend_keep = try relayFixedResponseBody(client_fd, backend_fd, hbuf[t + 4 .. hlen], cl);
    } else {
        // EOF-framed: relay until the backend closes. Can't reuse it, and the
        // client can't delimit the body, so both hops close.
        relayUntilEof(client_fd, backend_fd, hbuf[t + 4 .. hlen]);
        backend_keep = false;
    }

    backend_keep = backend_keep and !response_conn_close;
    return .{ .client_keep = client_keep, .backend_keep = backend_keep };
}

fn relayFixedResponseBody(client_fd: posix.fd_t, backend_fd: posix.fd_t, tail: []const u8, content_length: u64) !bool {
    var remaining: u64 = content_length;
    const from_tail = @min(@as(u64, tail.len), remaining);
    if (from_tail > 0) try writeAll(client_fd, tail[0..from_tail]);
    remaining -= from_tail;

    var pump: [4096]u8 = undefined;
    while (remaining > 0) {
        const want = pump[0..@min(@as(usize, pump.len), @as(usize, @intCast(remaining)))];
        const n = try posix.read(backend_fd, want);
        if (n == 0) return false; // backend closed early -> don't reuse
        try writeAll(client_fd, want[0..n]);
        remaining -= n;
    }
    return true; // read all Content-Length bytes; socket boundary is clean
}

fn relayChunkedResponseBody(client_fd: posix.fd_t, backend_fd: posix.fd_t, tail: []const u8) !bool {
    var scan: Chunked.Scanner = .{};
    var feed = tail;
    var pump: [4096]u8 = undefined;
    while (!scan.done) {
        const res = try scan.feed(feed);
        try writeAll(client_fd, feed[0..res.consumed]);
        if (res.done) return true; // reached the terminal chunk; boundary clean
        const n = try posix.read(backend_fd, &pump);
        if (n == 0) return false; // closed mid-chunked-body -> don't reuse
        feed = pump[0..n];
    }
    unreachable;
}

/// Relays the tail then keeps reading the backend to EOF. Used only for
/// close-delimited (framing-less) bodies, so the socket is never reused.
fn relayUntilEof(client_fd: posix.fd_t, backend_fd: posix.fd_t, tail: []const u8) void {
    if (tail.len > 0) writeAll(client_fd, tail) catch {};
    var pump: [4096]u8 = undefined;
    while (true) {
        const n = posix.read(backend_fd, &pump) catch return;
        if (n == 0) return;
        writeAll(client_fd, pump[0..n]) catch return;
    }
}

/// Connects to an Io.net.IpAddress (already resolved by balancer.zig) over a
/// raw std.posix socket. Handles both IPv4 and IPv6 backends.
fn containsTokenIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0 or haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

/// Whether a response status line ("HTTP/1.1 204 No Content") denotes a
/// message that carries no body — so no body bytes follow the head even if a
/// Content-Length header is present.
fn statusHasNoBody(status_line: []const u8) bool {
    var parts = std.mem.splitScalar(u8, status_line, ' ');
    _ = parts.next() orelse return false; // "HTTP/1.1"
    const code_str = parts.next() orelse return false;
    const code = std.fmt.parseInt(u16, code_str, 10) catch return false;
    return code < 200 or code == 204 or code == 304;
}

fn connectTo(addr: net.IpAddress) !posix.fd_t {
    // Disable Nagle on backend sockets: small keep-alive requests otherwise
    // stall ~40ms against the peer's delayed ACK. nginx does the same.
    switch (addr) {
        .ip4 => |ip4| {
            const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
            errdefer sysClose(fd);
            enableTcpNoDelay(fd);
            const sa: posix.sockaddr.in = .{
                .family = posix.AF.INET,
                .port = std.mem.nativeToBig(u16, ip4.port),
                .addr = @bitCast(ip4.bytes),
                .zero = [_]u8{0} ** 8,
            };
            try check(linux.connect(fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.in)));
            return fd;
        },
        .ip6 => |ip6| {
            const fd = try sysSocket(posix.AF.INET6, posix.SOCK.STREAM, posix.IPPROTO.TCP);
            errdefer sysClose(fd);
            enableTcpNoDelay(fd);
            const sa: posix.sockaddr.in6 = .{
                .family = posix.AF.INET6,
                .port = std.mem.nativeToBig(u16, ip6.port),
                .flowinfo = 0,
                .addr = ip6.bytes,
                .scope_id = 0,
            };
            try check(linux.connect(fd, @ptrCast(&sa), @sizeOf(posix.sockaddr.in6)));
            return fd;
        },
    }
}

fn writeAll(fd: posix.fd_t, bytes: []const u8) !void {
    var sent: usize = 0;
    while (sent < bytes.len) {
        sent += try sysWrite(fd, bytes[sent..]);
    }
}

fn sendSimple(fd: posix.fd_t, status: []const u8) void {
    var buf: [128]u8 = undefined;
    const msg = std.fmt.bufPrint(
        &buf,
        "HTTP/1.1 {s}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        .{status},
    ) catch return;
    writeAll(fd, msg) catch {};
}
