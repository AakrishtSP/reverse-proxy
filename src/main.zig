const std = @import("std");
const Config = @import("config.zig");
const StdIo = @import("stdio.zig").StdIo;
const Router = @import("router.zig");
const Server = @import("threaded/server.zig");
pub fn main(init: std.process.Init) !void {
    var stdio = StdIo.init(init);
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer {
        const status = gpa.deinit();
        if (status == .leak) std.debug.print("Warning: Memory leak found\n", .{});
    }

    const allocator = gpa.allocator();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var path_buf: [std.posix.PATH_MAX]u8 = undefined;
    const path = Config.resolveConfigPath(init, &path_buf) catch |err| switch (err) {
        error.FileNotFound => {
            try stdio.eprintln("Error: File not Found", .{});
            try stdio.println("Do you want to create a default config? (y/n) ", .{});
            try stdio.flush();
            const answer = try stdio.readChar();
            if (answer != 'y') return;
            return Config.writeDefaultConfig(init, "config.zon") catch |er| {
                try stdio.eprintln("Error: {any}\n", .{er});
                return er;
            };
        },
        else => return err,
    };
    const config = try Config.load(init, arena_allocator, path);
    try Config.printConfig(config, &stdio);

    var router = try Router.build(init.io, arena_allocator, config);
    try Server.run(arena_allocator, &router, config.listen);
}

test "simple test" {
    const gp = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gp); // Try commenting this out and see if zig detects the memory leak!
    try list.append(gp, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
