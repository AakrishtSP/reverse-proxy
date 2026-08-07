//! Chunked transfer body *scanner*: finds where a chunked body ends without
//! decoding it. A proxy relays the chunked bytes verbatim to the backend, but
//! still needs to know when "0\r\n[trailers]\r\n" terminates the body so it
//! can stop forwarding and, on a keep-alive connection, resume reading the
//! next request without eating its first bytes.
//!
//! Design: the caller streams bytes through `feed` and forwards whatever it
//! reports as consumed. The scanner holds only a small residual state (an
//! incomplete chunk-size / trailer line, plus "how many data bytes of the
//! current chunk remain"), so the body itself is never buffered — a large
//! upload streams through a copy of fixed size. `scanned`-style re-scanning is
//! not needed because the consumed-bytes rule is unambiguous from the framing:
//! a bare `0\r\n\r\n` byte run *inside* chunk data must not count as a
//! terminator, which is exactly the shape the state machine below prevents.

const std = @import("std");

pub const max_line = 4096; // chunk size + trailer lines are bounded & small

pub const FeedResult = struct {
    /// Bytes of `input` that belong to the chunked body (through the
    /// terminator). When `done` is false this always equals `input.len`.
    consumed: usize,
    /// True once the terminal chunk (0 + optional trailers + blank line) has
    /// been fully read.
    done: bool,
};

pub const Error = error{ BadChunkedBody, ChunkLineTooLong };

const Phase = enum { size, data, data_crlf, trailer };

pub const Scanner = struct {
    /// A partial line carried across `feed` calls (chunk-size and trailer
    /// lines can straddle reads; data? bytes are counted, not stored).
    line: [max_line]u8 = undefined,
    line_len: usize = 0,
    chunk_remaining: u64 = 0,
    phase: Phase = .size,
    done: bool = false,

    pub fn feed(self: *Scanner, input: []const u8) Error!FeedResult {
        var i: usize = 0;
        while (i < input.len) {
            switch (self.phase) {
                .size, .data_crlf, .trailer => {
                    if (self.line_len >= self.line.len) return error.ChunkLineTooLong;
                    self.line[self.line_len] = input[i];
                    self.line_len += 1;
                    i += 1;

                    const ends = self.line_len >= 2 and
                        self.line[self.line_len - 2] == '\r' and
                        self.line[self.line_len - 1] == '\n';
                    if (!ends) continue;

                    const line = self.line[0 .. self.line_len - 2];
                    self.line_len = 0;
                    switch (self.phase) {
                        .size => {
                            const n = try parseChunkSize(line);
                            if (n == 0) {
                                self.phase = .trailer;
                            } else {
                                self.chunk_remaining = n;
                                self.phase = .data;
                            }
                        },
                        .data_crlf => self.phase = .size, // the CRLF after a chunk's data
                        .trailer => if (line.len == 0) {
                            self.done = true;
                            return .{ .consumed = i, .done = true };
                        },
                        else => unreachable,
                    }
                },
                .data => {
                    const skip = @min(self.chunk_remaining, input.len - i);
                    self.chunk_remaining -= skip;
                    i += skip;
                    if (self.chunk_remaining == 0) self.phase = .data_crlf;
                },
            }
        }
        return .{ .consumed = i, .done = self.done };
    }
};

/// "4abc\r\n" -> size; chunk extensions after a ';' are ignored. Returns 0 for
/// the terminating empty chunk.
fn parseChunkSize(line: []const u8) Error!u64 {
    const before_ext = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
    const hex = std.mem.trim(u8, line[0..before_ext], " \t");
    if (hex.len == 0) return 0;
    return std.fmt.parseUnsigned(u64, hex, 16) catch return error.BadChunkedBody;
}

// Tests

const testing = std.testing;

fn scanAll(input: []const u8) !FeedResult {
    var s = Scanner{};
    return s.feed(input);
}

test "single chunk + terminator" {
    const r = try scanAll("5\r\nhello\r\n0\r\n\r\n");
    try testing.expect(r.done);
    try testing.expectEqual(@as(usize, 15), r.consumed); // "5\r\nhello\r\n0\r\n\r\n"
}

test "chunk-ext on the chunk-size line is stripped" {
    const r = try scanAll("5;foo=bar\r\nhello\r\n0\r\n\r\n");
    try testing.expect(r.done);
}

test "malformed chunk size rejected" {
    var s = Scanner{};
    try testing.expectError(error.BadChunkedBody, s.feed("zz\r\n"));
}

test "a terminator byte-pattern inside chunk data is not mistaken" {
    // "0\r\n\r\n" appears inside the 6-byte chunk but is NOT the terminator;
    // the body only ends after the real size=0 chunk.
    const r = try scanAll("6\r\n0\r\n\r\n!\r\n0\r\n\r\n");
    try testing.expect(r.done);
    try testing.expectEqual(@as(usize, 16), r.consumed);
}

test "streamed across reads finds terminator in later feed" {
    var s = Scanner{};
    const full = "3\r\nabc\r\n0\r\n\r\n";
    const r1 = try s.feed(full[0..4]); // partial size line
    try testing.expect(!r1.done);
    try testing.expectEqual(@as(usize, 4), r1.consumed);
    const r2 = try s.feed(full[4..]);
    try testing.expect(r2.done);
    try testing.expectEqual(@as(usize, full.len - 4), r2.consumed);
}

test "trailers followed by blank line terminate" {
    const r = try scanAll("0\r\nX-Trailer: v\r\n\r\n");
    try testing.expect(r.done);
}