const std = @import("std");
const Init = std.process.Init;
const StdIo = @import("stdio.zig").StdIo;

pub const VhostConfig = struct {
    hostnames: []const []const u8,
    backends: []const []const u8,
};

pub const Config = struct {
    listen: []const u8,
    vhosts: []const VhostConfig,
    default_backends: ?[]const []const u8 = null,
};

const default_config_zon =
    \\.{
    \\    .listen = "0.0.0.0:8080",
    \\    .vhosts = .{},
    \\    .default_backends = null,
    \\}
    \\
;

pub fn load(init: Init, allocator: std.mem.Allocator, path: []const u8) !Config {
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .unlimited);
    defer allocator.free(raw);
    const trimmed = std.mem.trim(u8, raw, " \t\n\r");
    const source = try allocator.dupeZ(u8, trimmed);
    defer allocator.free(source);

    var diag: std.zon.parse.Diagnostics = .{};
    defer diag.deinit(allocator);
    return std.zon.parse.fromSliceAlloc(Config, allocator, source, &diag, .{}) catch |err| {
        std.debug.print("config: failed to parse {s}: {any}\n{f}\n", .{ path, err, diag });
        return err;
    };
}

fn exists(init: Init, path: []const u8) !bool {
    std.Io.Dir.cwd().access(init.io, path, .{ .read = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

pub fn resolveConfigPath(init: Init, buf: []u8) ![]const u8 {
    const home = init.environ_map.get("HOME") orelse "";

    if (init.environ_map.get("XDG_CONFIG_HOME")) |xdg| {
        const path = try std.fmt.bufPrint(
            buf,
            "{s}/reverse-proxy/config.zon",
            .{xdg},
        );
        if (try exists(init, path)) return path;
    }

    inline for ([_][]const u8{
        "{s}/.config/reverse-proxy/config.zon",
        "{s}/.reverse-proxy/config.zon",
    }) |fmt| {
        const path = try std.fmt.bufPrint(buf, fmt, .{home});
        if (try exists(init, path)) return path;
    }

    if (try exists(init, "config.zon"))
        return "config.zon";

    return error.FileNotFound;
}

pub fn writeDefaultConfig(init: Init, path: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(init.io, path, .{ .exclusive = true });
    defer file.close(init.io);
    file.writeStreamingAll(init.io, default_config_zon) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        return err;
    };
}

pub fn printConfig(config: Config, stdio: *StdIo) !void {
    try stdio.println(".{{", .{});
    try stdio.println("\t.listen: \"{s}\",", .{config.listen});
    try stdio.println("\t.vhosts: \n\t.{{", .{});
    for (config.vhosts) |vhost| {
        try stdio.print("\t\t.{{\n\t\t\t.hostnames: .{{", .{});
        for (vhost.hostnames) |hostname| {
            try stdio.print("\t\"{s}\", ", .{hostname});
        }
        try stdio.print("}}\n\t\t\t.backends: .{{", .{});
        for (vhost.backends) |backend| {
            try stdio.print("\t\"{s}\", ", .{backend});
        }
        try stdio.println("}}\n\t\t}}", .{});
    }

    try stdio.println("\t}}\n}}", .{});
    try stdio.flush();
}
