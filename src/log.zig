//! Background async logging.
//!
//! The hot path calls `log.fmt(...)` which only formats into a stack buffer and
//! copies it into a bounded ring under a spinlock — no syscall, no I/O, and if
//! the ring is contended-or-full the line is dropped rather than blocking. A
//! dedicated worker thread polls the ring and writes batches to stderr, so
//! logging can never stall the request path.
//!
//! `logger` is a process-wide singleton shared by both servers (and main).
//! Only `start`/`deinit` touch the thread; every `fmt` site is fire-and-forget.
const std = @import("std");
const linux = std.os.linux;

const cap = 256; // ring slots — error/notice lines, a flood simply drops
const line_len = 256; // max bytes per line (truncated otherwise)

pub var logger: Logger = .{};

/// Module-level convenience: `log.fmt(fmt, args)` targets the global logger.
pub fn fmt(comptime format: []const u8, args: anytype) void {
    logger.fmt(format, args);
}

pub const Logger = struct {
    lock: std.atomic.Mutex = .unlocked,
    slots: [cap][line_len]u8 = undefined,
    len: [cap]usize = undefined,
    next: usize = 0, // next slot to fill
    count: usize = 0, // buffered, not-yet-written lines
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?std.Thread = null,

    /// Spawn the background writer. Idempotent; called once from main.
    pub fn start(self: *Logger) void {
        self.thread = std.Thread.spawn(.{}, drain, .{self}) catch null;
    }

    /// Enqueue a line. Non-blocking: formats into a stack buffer, then under a
    /// (brief) spinlock copies into the ring. Drops if the lock is contended or
    /// the ring is full — a logging backlog must never back up the proxy.
    pub fn fmt(self: *Logger, comptime format: []const u8, args: anytype) void {
        var buf: [line_len]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, format ++ "\n", args) catch return;
        if (!self.lock.tryLock()) return; // contended — drop rather than block
        defer self.lock.unlock();
        if (self.count == cap) return; // full — drop the newest
        @memcpy(self.slots[self.next][0..line.len], line);
        self.len[self.next] = line.len;
        self.next = (self.next + 1) % cap;
        self.count += 1;
    }

    /// Stop the worker, flush anything still buffered. Call on shutdown.
    pub fn deinit(self: *Logger) void {
        self.stop.store(true, .release);
        if (self.thread) |t| t.join();
        self.thread = null;
        while (!self.lock.tryLock()) {
            _ = linux.clock_nanosleep(linux.CLOCK.REALTIME, .{ .ABSTIME = false }, &drain_ts, null);
        }
        defer self.lock.unlock();
        var idx = (self.next + cap - self.count) % cap;
        while (self.count > 0) : (self.count -= 1) {
            writeStderr(self.slots[idx][0..self.len[idx]]);
            idx = (idx + 1) % cap;
        }
    }

    fn drain(self: *Logger) void {
        while (true) {
            while (!self.lock.tryLock()) {
                _ = linux.clock_nanosleep(linux.CLOCK.REALTIME, .{ .ABSTIME = false }, &drain_ts, null);
            }
            if (self.count == 0) {
                const done = self.stop.load(.acquire);
                self.lock.unlock();
                if (done) return;
                _ = linux.clock_nanosleep(linux.CLOCK.REALTIME, .{ .ABSTIME = false }, &drain_ts, null);
                continue;
            }
            var idx = (self.next + cap - self.count) % cap;
            while (self.count > 0) : (self.count -= 1) {
                writeStderr(self.slots[idx][0..self.len[idx]]);
                idx = (idx + 1) % cap;
            }
            self.lock.unlock();
        }
    }
};

const drain_ts = std.posix.timespec{ .sec = 0, .nsec = 20 * std.time.ns_per_ms };

/// Raw write to fd 2 (stderr) — avoids the async `std.Io` model entirely and
/// never panics; a failed log write is dropped.
fn writeStderr(msg: []const u8) void {
    _ = linux.write(2, msg.ptr, msg.len);
}
