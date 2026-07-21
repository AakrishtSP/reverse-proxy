const std = @import("std");

pub const max_headers = 64;

pub const ParseError = error{
    BadRequestLine,
    BadVersion,
    BadHeaderLine,
    TooManyHeaders,
    ConflictingFraming, // duplicate/conflicting Content-Length, or CL + Transfer-Encoding together
    BadContentLength,
};

pub const Version = enum { http_1_0, http_1_1 };

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Request = struct {
    method: []const u8,
    target: []const u8,
    version: Version,
    headers: []const Header,
    content_length: ?u64,
    chunked: bool,
    consumed: usize, // bytes of `buf` this request occupied, incl. the trailing CRLFCRLF

    pub fn header(self: Request, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

// RFC 7230 6.1 base hop-by-hop headers. Any header additionally named in a
// Connection header value is also hop-by-hop for that message.
const base_hop_by_hop = [_][]const u8{
    "connection",          "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te",         "trailer",
    "transfer-encoding",   "upgrade",
};

pub const Parser = struct {
    scanned: usize = 0,
    header_buf: [max_headers]Header = undefined,

    pub fn next(self: *Parser, buf: []const u8) ParseError!?Request {
        const end = std.mem.indexOfPos(u8, buf, self.scanned, "\r\n\r\n") orelse {
            // Rewind 3 bytes: a CRLFCRLF straddling this call's boundary
            // (some of it arrived last call, the rest arrives next call)
            // must not be missed.
            self.scanned = if (buf.len >= 3) buf.len - 3 else 0;
            return null;
        };
        self.scanned = 0;

        const head = buf[0..end]; // request-line + headers, sans the final blank-line CRLFCRLF
        var lines = std.mem.splitSequence(u8, head, "\r\n");

        const request_line = lines.next() orelse return error.BadRequestLine;
        var parts = std.mem.splitScalar(u8, request_line, ' ');
        const method = parts.next() orelse return error.BadRequestLine;
        const target = parts.next() orelse return error.BadRequestLine;
        const version_str = parts.next() orelse return error.BadRequestLine;
        if (parts.next() != null or method.len == 0 or target.len == 0) return error.BadRequestLine;
        const version = try parseVersion(version_str);

        var count: usize = 0;
        var connection_tokens: [8][]const u8 = undefined;
        var connection_token_count: usize = 0;
        var content_length: ?u64 = null;
        var chunked = false;

        while (lines.next()) |line| {
            // obs-fold (header continuation lines starting with whitespace) is
            // obsolete per RFC 7230 3.2.4 and rejected below via BadHeaderLine
            // (no colon on the continuation line) rather than supported.
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.BadHeaderLine;
            const name = line[0..colon];
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
            if (name.len == 0) return error.BadHeaderLine;
            for (name) |c| {
                // No whitespace allowed before the colon — permitting it is a
                // known HTTP request-smuggling vector (proxies disagreeing on
                // where a header name ends).
                if (c == ' ' or c == '\t') return error.BadHeaderLine;
            }

            if (std.ascii.eqlIgnoreCase(name, "content-length")) {
                if (content_length != null) return error.ConflictingFraming; // duplicate CL
                content_length = std.fmt.parseUnsigned(u64, value, 10) catch return error.BadContentLength;
            } else if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
                if (containsTokenIgnoreCase(value, "chunked")) chunked = true;
            } else if (std.ascii.eqlIgnoreCase(name, "connection")) {
                var toks = std.mem.splitScalar(u8, value, ',');
                while (toks.next()) |t| {
                    const trimmed = std.mem.trim(u8, t, " \t");
                    if (trimmed.len == 0) continue;
                    if (connection_token_count < connection_tokens.len) {
                        connection_tokens[connection_token_count] = trimmed;
                        connection_token_count += 1;
                    }
                }
            }

            if (count >= max_headers) return error.TooManyHeaders;
            self.header_buf[count] = .{ .name = name, .value = value };
            count += 1;
        }

        if (content_length != null and chunked) return error.ConflictingFraming;

        // Compact header_buf in place, dropping hop-by-hop headers.
        var out: usize = 0;
        for (self.header_buf[0..count]) |h| {
            if (isHopByHop(h.name, connection_tokens[0..connection_token_count])) continue;
            self.header_buf[out] = h;
            out += 1;
        }

        return Request{
            .method = method,
            .target = target,
            .version = version,
            .headers = self.header_buf[0..out],
            .content_length = content_length,
            .chunked = chunked,
            .consumed = end + 4,
        };
    }
};

fn parseVersion(s: []const u8) ParseError!Version {
    if (std.mem.eql(u8, s, "HTTP/1.1")) return .http_1_1;
    if (std.mem.eql(u8, s, "HTTP/1.0")) return .http_1_0;
    return error.BadVersion;
}

fn isHopByHop(name: []const u8, extra: []const []const u8) bool {
    for (base_hop_by_hop) |h| {
        if (std.ascii.eqlIgnoreCase(name, h)) return true;
    }
    for (extra) |h| {
        if (std.ascii.eqlIgnoreCase(name, h)) return true;
    }
    return false;
}

fn containsTokenIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0 or haystack.len < needle.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

// Tests

const testing = std.testing;

test "simple GET, no headers, no body" {
    var p = Parser{};
    const buf = "GET / HTTP/1.1\r\n\r\n";
    const req = (try p.next(buf)).?;
    try testing.expectEqualStrings("GET", req.method);
    try testing.expectEqualStrings("/", req.target);
    try testing.expectEqual(Version.http_1_1, req.version);
    try testing.expectEqual(@as(usize, 0), req.headers.len);
    try testing.expectEqual(@as(?u64, null), req.content_length);
    try testing.expectEqual(false, req.chunked);
    try testing.expectEqual(buf.len, req.consumed);
}

test "headers parsed, hop-by-hop stripped" {
    var p = Parser{};
    const buf = "GET /x HTTP/1.1\r\n" ++
        "Host: example.com\r\n" ++
        "Connection: close, X-Custom\r\n" ++
        "X-Custom: should-be-stripped\r\n" ++
        "Content-Length: 5\r\n" ++
        "\r\n";
    const req = (try p.next(buf)).?;
    try testing.expectEqual(@as(?u64, 5), req.content_length);
    try testing.expectEqualStrings("example.com", req.header("host").?);
    try testing.expectEqual(@as(?[]const u8, null), req.header("connection")); // hop-by-hop
    try testing.expectEqual(@as(?[]const u8, null), req.header("x-custom")); // named by Connection
    try testing.expectEqual(@as(usize, 2), req.headers.len); // Host + Content-Length only
}

test "chunked framing detected, Transfer-Encoding stripped as hop-by-hop" {
    var p = Parser{};
    const buf = "POST /up HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
    const req = (try p.next(buf)).?;
    try testing.expectEqual(true, req.chunked);
    try testing.expectEqual(@as(?u64, null), req.content_length);
    try testing.expectEqual(@as(?[]const u8, null), req.header("transfer-encoding"));
}

test "Content-Length and Transfer-Encoding together is rejected" {
    var p = Parser{};
    const buf = "POST /up HTTP/1.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n";
    try testing.expectError(error.ConflictingFraming, p.next(buf));
}

test "duplicate Content-Length is rejected" {
    var p = Parser{};
    const buf = "POST /up HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n";
    try testing.expectError(error.ConflictingFraming, p.next(buf));
}

test "malformed request line rejected" {
    var p = Parser{};
    try testing.expectError(error.BadRequestLine, p.next("GET /nourl\r\n\r\n"));
}

test "header line with no colon rejected" {
    var p = Parser{};
    try testing.expectError(error.BadHeaderLine, p.next("GET / HTTP/1.1\r\nBadHeader\r\n\r\n"));
}

test "whitespace before colon in header name rejected (smuggling guard)" {
    var p = Parser{};
    try testing.expectError(error.BadHeaderLine, p.next("GET / HTTP/1.1\r\nHost : example.com\r\n\r\n"));
}

test "too many headers rejected" {
    var p = Parser{};
    var buf: [8192]u8 = undefined;
    var len: usize = 0;

    const request_line = "GET / HTTP/1.1\r\n";
    std.mem.copyForwards(u8, buf[len..], request_line);
    len += request_line.len;

    const line = "X-Custom-Header-Name: v\r\n";
    var i: usize = 0;
    while (i < max_headers + 1) : (i += 1) {
        std.mem.copyForwards(u8, buf[len..], line);
        len += line.len;
    }

    std.mem.copyForwards(u8, buf[len..], "\r\n");
    len += 2;

    try testing.expectError(error.TooManyHeaders, p.next(buf[0..len]));
}

test "resumable across partial reads" {
    var p = Parser{};
    const full = "GET /partial HTTP/1.1\r\nHost: a\r\n\r\n";
    const split_at = 10; // lands mid request-line, well before headers end

    // First feed only the first chunk: not enough data yet.
    try testing.expectEqual(@as(?Request, null), try p.next(full[0..split_at]));
    // Then feed the rest, still starting from byte 0 (same buffer, grown).
    const req = (try p.next(full)).?;
    try testing.expectEqualStrings("GET", req.method);
    try testing.expectEqualStrings("/partial", req.target);
    try testing.expectEqual(full.len, req.consumed);
}

test "CRLFCRLF split exactly across two calls is not missed" {
    var p = Parser{};
    const full = "GET / HTTP/1.1\r\n\r\n";
    const split_at = full.len - 2; // splits right inside the trailing CRLFCRLF

    try testing.expectEqual(@as(?Request, null), try p.next(full[0..split_at]));
    const req = (try p.next(full)).?;
    try testing.expectEqualStrings("GET", req.method);
}
