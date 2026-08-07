//! Pure HTTP header/socket-formatting helpers shared by the threaded and
//! epoll server backends, so forwarded-for formatting and Host normalization
//! stay consistent across both.
const std = @import("std");

/// Writes the X-Forwarded-For header for a raw v6 sockaddr's 16-byte
/// address, unmapping v4-mapped addresses (::ffff:a.b.c.d) back to plain
/// dotted-quad since that's what downstream log parsers expect from a v4
/// client — the listener being dual-stack shouldn't leak into the header
/// shape. Returns the new head_len.
pub fn writeForwardedFor(head: []u8, head_len: usize, addr: [16]u8) !usize {
    const is_v4_mapped = std.mem.eql(u8, addr[0..10], &[_]u8{0} ** 10) and
        addr[10] == 0xff and addr[11] == 0xff;
    if (is_v4_mapped) {
        return head_len + (try std.fmt.bufPrint(
            head[head_len..],
            "X-Forwarded-For: {d}.{d}.{d}.{d}\r\n",
            .{ addr[12], addr[13], addr[14], addr[15] },
        )).len;
    }
    // ponytail: expanded form only (no :: compression, no leading-zero
    // trimming) — correct but ugly. Fine for a header nothing parses
    // strictly. Upgrade path: std.net.Ip6Address's formatter if/once it's
    // stable again post-0.16 std.Io churn.
    const groups: [8]u16 = @bitCast(addr);
    return head_len + (try std.fmt.bufPrint(
        head[head_len..],
        "X-Forwarded-For: {x}:{x}:{x}:{x}:{x}:{x}:{x}:{x}\r\n",
        .{
            std.mem.bigToNative(u16, groups[0]), std.mem.bigToNative(u16, groups[1]),
            std.mem.bigToNative(u16, groups[2]), std.mem.bigToNative(u16, groups[3]),
            std.mem.bigToNative(u16, groups[4]), std.mem.bigToNative(u16, groups[5]),
            std.mem.bigToNative(u16, groups[6]), std.mem.bigToNative(u16, groups[7]),
        },
    )).len;
}

/// Strips a ":port" suffix (or keeps a "[ipv6]" literal intact) so the
/// result matches what Router.route expects.
pub fn hostOnly(h: []const u8) []const u8 {
    if (h.len > 0 and h[0] == '[') {
        const end = std.mem.indexOfScalar(u8, h, ']') orelse return h;
        return h[0 .. end + 1];
    }
    const colon = std.mem.lastIndexOfScalar(u8, h, ':') orelse return h;
    return h[0..colon];
}

/// Whether a response status line ("HTTP/1.1 204 No Content") denotes a
/// message that carries no body — so no body bytes follow the head even if a
/// Content-Length header is present.
pub fn statusHasNoBody(status_line: []const u8) bool {
    var parts = std.mem.splitScalar(u8, status_line, ' ');
    _ = parts.next() orelse return false; // "HTTP/1.1"
    const code_str = parts.next() orelse return false;
    const code = std.fmt.parseInt(u16, code_str, 10) catch return false;
    return code < 200 or code == 204 or code == 304;
}

/// Token scan over a header value (e.g. "close, X-Foo" contains "close").
pub fn containsTokenIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0 or haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

test "v4-mapped address unmaps to dotted-quad in X-Forwarded-For" {
    var head: [128]u8 = undefined;
    const mapped: [16]u8 = [_]u8{0} ** 10 ++ [_]u8{ 0xff, 0xff, 10, 0, 0, 1 };
    const len = try writeForwardedFor(&head, 0, mapped);
    try std.testing.expectEqualStrings("X-Forwarded-For: 10.0.0.1\r\n", head[0..len]);
}

test "real v6 address stays expanded hex, not misread as v4" {
    var head: [128]u8 = undefined;
    const real_v6: [16]u8 = [_]u8{ 0x20, 0x01 } ++ [_]u8{0} ** 14;
    const len = try writeForwardedFor(&head, 0, real_v6);
    try std.testing.expect(std.mem.startsWith(u8, head[0..len], "X-Forwarded-For: 2001:"));
}

test "statusHasNoBody collapses no-body statuses" {
    try std.testing.expect(statusHasNoBody("HTTP/1.1 204 No Content"));
    try std.testing.expect(statusHasNoBody("HTTP/1.1 304 Not Modified"));
    try std.testing.expect(statusHasNoBody("HTTP/1.1 101 Switching Protocols"));
    try std.testing.expect(!statusHasNoBody("HTTP/1.1 200 OK"));
    try std.testing.expect(!statusHasNoBody("HTTP/1.1 404 Not Found"));
}

test "containsTokenIgnoreCase matches tokens" {
    try std.testing.expect(containsTokenIgnoreCase("close, X-Foo", "close"));
    try std.testing.expect(!containsTokenIgnoreCase("keep-alive", "close"));
    try std.testing.expect(containsTokenIgnoreCase("keep-alive, upgrade", "Upgrade"));
}
