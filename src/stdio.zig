const std = @import("std");

pub const StdIo = struct {
    stdin_reader: std.Io.File.Reader,
    stdout_writer: std.Io.File.Writer,
    stderr_writer: std.Io.File.Writer,

    stdin_buf: [1024]u8 = undefined,
    stdout_buf: [4096]u8 = undefined,
    stderr_buf: [1024]u8 = undefined,

    pub fn init(proc: std.process.Init) StdIo {
        var self: StdIo = undefined;

        self.stdin_reader = std.Io.File.stdin().reader(proc.io, &self.stdin_buf);
        self.stdout_writer = std.Io.File.stdout().writer(proc.io, &self.stdout_buf);
        self.stderr_writer = std.Io.File.stderr().writer(proc.io, &self.stderr_buf);

        return self;
    }

    pub fn flush(self: *StdIo) !void {
        try self.stdout().flush();
        try self.stderr().flush();
    }
    fn stdin(self: *StdIo) *std.Io.Reader {
        return &self.stdin_reader.interface;
    }

    fn stdout(self: *StdIo) *std.Io.Writer {
        return &self.stdout_writer.interface;
    }

    fn stderr(self: *StdIo) *std.Io.Writer {
        return &self.stderr_writer.interface;
    }
    pub fn print(self: *StdIo, comptime fmt: []const u8, args: anytype) !void {
        try self.stdout().print(fmt, args);
    }

    pub fn eprint(self: *StdIo, comptime fmt: []const u8, args: anytype) !void {
        try self.stderr().print(fmt, args);
    }

    pub fn println(self: *StdIo, comptime fmt: []const u8, args: anytype) !void {
        try self.stdout().print(fmt ++ "\n", args);
    }

    pub fn eprintln(self: *StdIo, comptime fmt: []const u8, args: anytype) !void {
        try self.stderr().print(fmt ++ "\n", args);
    }

    pub fn readLine(self: *StdIo) ![]const u8 {
        return self.stdin().takeDelimiterExclusive('\n');
    }
    pub fn readChar(self: *StdIo) !u8 {
        var ch: [1]u8 = undefined;
        _ = try self.stdin().readSliceShort(&ch);
        return ch[0];
    }
};
