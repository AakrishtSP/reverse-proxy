//! Thread-per-connection HTTP/1.1 proxy loop.
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
//! Current scope (deliberately narrow — see phase-2 epoll work for the rest):
//!   - One request per connection. No pipelining/keep-alive to the client yet;
//!     each accepted connection handles exactly one request/response then closes.
//!   - Content-Length request bodies only. Chunked request bodies are refused
//!     with 501 — forwarding them correctly needs http/chunked.zig's boundary
//!     scanner (not built yet) to know where the body ends without decoding it.
//!   - The backend response is relayed as raw bytes, unparsed, until the
//!     backend closes its end. We always send it "Connection: close", so a
//!     well-behaved backend closing after the response is what ends the relay.
//!     ponytail: a misbehaving backend that ignores Connection: close and
//!     keeps the socket open would hang this relay until the OS times it out.
//!     Upgrade path: parse the backend's own Content-Length/chunked framing
//!     instead of relying on it to close.
//!   - IPv4 only on the listen side (matches the hardcoded 0.0.0.0 bind).
//!     Backends may still be IPv6 (connectTo handles both) since that comes
//!     from balancer.zig's existing resolution.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const net = std.Io.net; // type-only: IpAddress from balancer.zig, no I/O calls

const http = @import("../http/parser.zig");
const Parser = http.Parser;
const Request = http.Request;

const RouterMod = @import("../router.zig");
const Router = RouterMod.Router;
const Balancer = @import("../balancer.zig").Balancer;

const max_request_head = 8192;

/// std.posix.{socket,bind,listen,accept,connect} were removed in 0.16
/// (moved into std.Io.net's vtable). We already can't route through
/// std.Io.net.Stream here (see file header — it drops writes), so this
/// drops one layer lower to the raw std.os.linux syscalls std.posix used
/// to wrap, and does its own errno check. read/write/close/setsockopt are
/// untouched — those stayed in std.posix.
fn check(rc: usize) !void {
    return switch (posix.errno(rc)) {
        .SUCCESS => {},
        // EPIPE is routine for a proxy — the client or backend can hang up
        // mid-write at any time, especially under load — not a bug. Give it
        // its own error instead of routing it through unexpectedErrno's
        // panic-and-dump-stack path, which is for genuinely unanticipated
        // errnos.
        .PIPE => error.BrokenPipe,
        // ponytail: every other setup errno collapses into error.Unexpected
        // instead of a precise set (AddressInUse, ConnectionRefused, ...).
        // Fine since every caller here just logs and bails. If a caller ever
        // needs to branch on a specific failure, give that call site its own
        // error set instead of widening this for everyone.
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

pub fn run(allocator: std.mem.Allocator, router: *Router, port: u16) !void {
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

    const listen_fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    defer sysClose(listen_fd);

    try posix.setsockopt(listen_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    const bind_addr: posix.sockaddr.in = .{
        .family = posix.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = 0, // INADDR_ANY (0.0.0.0)
        .zero = [_]u8{0} ** 8,
    };
    try check(linux.bind(listen_fd, @ptrCast(&bind_addr), @sizeOf(posix.sockaddr.in)));
    try check(linux.listen(listen_fd, 128));

    while (true) {
        var peer: posix.sockaddr.in = undefined;
        var peer_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        const conn_fd = sysAccept4(listen_fd, @ptrCast(&peer), &peer_len, posix.SOCK.CLOEXEC) catch |err| {
            std.debug.print("accept failed: {any}\n", .{err});
            continue;
        };
        const thread = std.Thread.spawn(.{}, handleConnection, .{ allocator, router, conn_fd, peer }) catch |err| {
            std.debug.print("thread spawn failed: {any}\n", .{err});
            sysClose(conn_fd);
            continue;
        };
        thread.detach();
    }
}

fn handleConnection(allocator: std.mem.Allocator, router: *Router, conn_fd: posix.fd_t, peer: posix.sockaddr.in) void {
    defer sysClose(conn_fd);

    var head_buf: [max_request_head]u8 = undefined;
    var len: usize = 0;
    var parser: Parser = .{};

    const req: Request = while (true) {
        if (len == head_buf.len) {
            sendSimple(conn_fd, "431 Request Header Fields Too Large");
            return;
        }

        const n = posix.read(conn_fd, head_buf[len..]) catch |err| {
            std.debug.print("read error: {any}\n", .{err});
            return;
        };
        if (n == 0) return; // peer closed before sending a full request

        len += n;

        if (parser.next(head_buf[0..len])) |maybe_req| {
            if (maybe_req) |r| break r;
            // else: headers not complete yet, read more
        } else |_| {
            sendSimple(conn_fd, "400 Bad Request");
            return;
        }
    };

    const host_header = req.header("host") orelse {
        sendSimple(conn_fd, "400 Bad Request");
        return;
    };
    const bal = router.route(hostOnly(host_header)) orelse return; // nginx `return 444`-style: deny by closing, no response

    forward(allocator, bal, conn_fd, peer, req, head_buf[req.consumed..len]) catch |err| {
        std.debug.print("proxy error: {any}\n", .{err});
    };
}

fn forward(
    allocator: std.mem.Allocator,
    bal: *Balancer,
    client_fd: posix.fd_t,
    peer: posix.sockaddr.in,
    req: Request,
    leftover: []const u8,
) !void {
    _ = allocator; // not needed yet — everything here is fixed-size buffers

    if (req.chunked) {
        sendSimple(client_fd, "501 Not Implemented");
        return;
    }

    const backend_addr = bal.pick();
    const backend_fd = try connectTo(backend_addr);
    defer sysClose(backend_fd);

    // Build the request head into one buffer, then write it in as few
    // syscalls as possible (writeAll below still loops for partial writes).
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
    const peer_ip: [4]u8 = @bitCast(peer.addr);
    head_len += (try std.fmt.bufPrint(
        head[head_len..],
        "X-Forwarded-For: {d}.{d}.{d}.{d}\r\n",
        .{ peer_ip[0], peer_ip[1], peer_ip[2], peer_ip[3] },
    )).len;
    head_len += (try std.fmt.bufPrint(head[head_len..], "Via: 1.1 reverse-proxy\r\n", .{})).len;
    head_len += (try std.fmt.bufPrint(head[head_len..], "Connection: close\r\n\r\n", .{})).len;

    try writeAll(backend_fd, head[0..head_len]);

    if (req.content_length) |content_length| {
        var remaining = content_length;

        const from_leftover = @min(leftover.len, remaining);
        if (from_leftover > 0) {
            try writeAll(backend_fd, leftover[0..from_leftover]);
            remaining -= from_leftover;
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

    // Relay the response back verbatim; we don't need to parse it, just
    // pump bytes until the backend closes (see file-level ponytail note).
    var pump: [4096]u8 = undefined;
    while (true) {
        const n = try posix.read(backend_fd, &pump);
        if (n == 0) break;
        try writeAll(client_fd, pump[0..n]);
    }
}

/// Connects to an Io.net.IpAddress (already resolved by balancer.zig) over a
/// raw std.posix socket. Handles both IPv4 and IPv6 backends.
fn connectTo(addr: net.IpAddress) !posix.fd_t {
    switch (addr) {
        .ip4 => |ip4| {
            const fd = try sysSocket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
            errdefer sysClose(fd);
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

/// Strips a ":port" suffix (or keeps a "[ipv6]" literal intact) so the
/// result matches what Router.route expects.
fn hostOnly(h: []const u8) []const u8 {
    if (h.len > 0 and h[0] == '[') {
        const end = std.mem.indexOfScalar(u8, h, ']') orelse return h;
        return h[0 .. end + 1];
    }
    const colon = std.mem.lastIndexOfScalar(u8, h, ':') orelse return h;
    return h[0..colon];
}
