const std = @import("std");
const builtin = @import("builtin");
const Config = @import("config.zig");
const StdIo = @import("stdio.zig").StdIo;
const Router = @import("router.zig");
const Server = @import("threaded/server.zig");
const EpollServer = @import("epoll/server.zig");
const logmod = @import("log.zig");
pub fn main(init: std.process.Init) !void {
    var stdio = StdIo.init(init);
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    var gpa_allocator: std.mem.Allocator = undefined;
    defer {
        if (builtin.mode == .Debug) {
            const status = gpa.deinit();
            if (status == .leak) std.debug.print("Warning: Memory leak found\n", .{});
        }
    }

    const allocator = if (builtin.mode == .Debug) blk: {
        gpa_allocator = gpa.allocator();
        break :blk gpa_allocator;
    } else std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var config_path: ?[]const u8 = null;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip(); // argv[0]
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "-c")) {
            config_path = args.next() orelse {
                try stdio.eprintln("-c requires a config path", .{});
                return error.Usage;
            };
        } else {
            try stdio.eprintln("usage: reverse_proxy [-c <config.zon>]", .{});
            return error.Usage;
        }
    }

    const path = if (config_path) |p|
        p
    else blk: {
        var path_buf: [std.posix.PATH_MAX]u8 = undefined;
        break :blk Config.resolveConfigPath(init, &path_buf) catch |err| switch (err) {
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
    };
    const config = try Config.load(init, arena_allocator, path);
    try Config.printConfig(config, &stdio);

    var router = try Router.build(init.io, arena_allocator, config);
    logmod.logger.start();
    defer logmod.logger.deinit();
    switch (config.server.kind) {
        .threaded => try Server.run(arena_allocator, &router, config.listen, config.server.threaded),
        // epoll preallocates a fixed pool of Conn slots (opts.max_connections)
        // and recycles them, so it needs no per-connection allocations at all;
        // the freeing allocator is still used for the backend-socket pool.
        .epoll => try EpollServer.run(allocator, &router, config.listen, config.server.epoll),
    }
}

test {
    std.testing.refAllDecls(@This());
}
