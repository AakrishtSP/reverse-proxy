const std = @import("std");
const Init = std.process.Init;
const StdIo = @import("stdio.zig").StdIo;

pub const VhostConfig = struct {
    hostnames: []const []const u8,
    backends: []const []const u8,
};

/// Options for the thread-per-connection backend. Buffer sizes stay
/// compile-time constants (they size stack arrays), so only the pool size is
/// runtime-tunable here.
pub const ThreadedConfig = struct {
    pool_threads: u16 = 16,
    /// Keep the client connection alive across requests when the client asks
    /// for it. When false, every response answers Connection: close and the
    /// socket is shut after one request.
    client_keepalive: bool = true,
    /// Reuse the backend connection across a client's keep-alive requests.
    /// When false, every request gets a fresh backend connection (and asks the
    /// backend to close it). Only pays off when the client keeps alive.
    backend_keepalive: bool = true,
    /// Relay chunked (Transfer-Encoding: chunked) request bodies. When false,
    /// chunked requests are rejected with 415 before reaching the backend.
    chunked_requests: bool = true,
};

pub const EpollConfig = struct {
    /// Keep the client connection alive across requests when the client asks
    /// for it and the response framing allows it.
    client_keepalive: bool = true,
    /// Reuse backend connections across requests via the shared pool. When
    /// false, every request gets a fresh backend connection (asked to close).
    backend_keepalive: bool = true,
    /// Relay chunked (Transfer-Encoding: chunked) request bodies. When false,
    /// chunked requests are rejected with 415 before reaching the backend.
    chunked_requests: bool = true,
    /// Max idle backend sockets the pool will keep open. Beyond this, returned
    /// connections are closed instead of pooled.
    pool_size: usize = 100,
    /// Fixed number of preallocated client connections (in-flight requests).
    /// Each slot owns its read/write buffers, so this bounds heap usage up
    /// front. When exhausted, new connections are refused until a slot frees.
    /// With `loops > 1` this total is split evenly across the loops.
    max_connections: usize = 2048,
    /// Number of independent epoll event loops. Each loop owns its own listen
    /// socket bound with SO_REUSEPORT and its own ConnPool partition, so the
    /// keep-alive (CPU-bound) path scales across cores. `1` is the classic
    /// single-loop server.
    loops: usize = 1,
};

pub const ServerConfig = struct {
    /// Which serving backend to use. `threaded` (pool of blocking worker
    /// threads, per-client backend reuse) and `epoll` (single-threaded event
    /// loop, shared backend connection pool) are both implemented. `epoll` is
    /// the default and fastest.
    kind: enum { threaded, epoll } = .epoll,
    threaded: ThreadedConfig = .{},
    epoll: EpollConfig = .{},
};

pub const Config = struct {
    listen: u16,
    server: ServerConfig = .{},
    vhosts: []const VhostConfig,
    default_backends: ?[]const []const u8 = null,
};

const default_config_zon =
    \\.{
    \\    .listen = 8080,
    \\    .server = .{
    \\        .kind = .epoll,
    \\        .epoll = .{
    \\            .client_keepalive = true,
    \\            .backend_keepalive = true,
    \\            .chunked_requests = true,
    \\            .pool_size = 100,
    \\            .max_connections = 2048,
    \\            .loops = 1,
    \\        },
    \\    },
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
    try stdio.println("\t.listen: {d},", .{config.listen});
    try stdio.println("\t.server: .{{", .{});
    try stdio.println("\t\t.kind: .{s},", .{@tagName(config.server.kind)});
    try stdio.println(
        "\t\t.threaded: .{{.pool_threads: {d}, .client_keepalive: {}, .backend_keepalive: {}, .chunked_requests: {}}},\n\t\t.epoll: .{{.client_keepalive: {}, .backend_keepalive: {}, .chunked_requests: {}, .pool_size: {d}, .max_connections: {d}, .loops: {d}}},\n\t}},\n\t.vhosts: ",
        .{
            config.server.threaded.pool_threads,
            config.server.threaded.client_keepalive,
            config.server.threaded.backend_keepalive,
            config.server.threaded.chunked_requests,
            config.server.epoll.client_keepalive,
            config.server.epoll.backend_keepalive,
            config.server.epoll.chunked_requests,
            config.server.epoll.pool_size,
            config.server.epoll.max_connections,
            config.server.epoll.loops,
        },
    );
    try stdio.println("\t.{{", .{});
    for (config.vhosts) |vhost| {
        try stdio.print("\t\t.{{\n\t\t\t.hostnames: .{{", .{});
        for (vhost.hostnames) |hostname| {
            try stdio.print("\t\"{s}\", ", .{hostname});
        }
        try stdio.print("}},\n\t\t\t.backends: .{{", .{});
        for (vhost.backends) |backend| {
            try stdio.print("\t\"{s}\", ", .{backend});
        }
        try stdio.println("}},\n\t\t}},", .{});
    }

    try stdio.println("\t}},", .{});
    try stdio.print("\t.default_backends: .{{", .{});
    if (config.default_backends) |backends| {
        for (backends) |backend| {
            try stdio.print("\t\"{s}\", ", .{backend});
        }
    }
    try stdio.print("}},\n}}", .{});
    try stdio.flush();
}
