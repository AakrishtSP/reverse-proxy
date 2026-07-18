const std = @import("std");
const Io = std.Io;
const Config = @import("config.zig").Config;
const balancer = @import("balancer.zig");
const Balancer = balancer.Balancer;

pub const VHost = struct {
    hostnames: []const []const u8,
    balancer: Balancer,
};

pub const Router = struct {
    vhosts: []VHost,
    default: ?Balancer,

    /// `hostname` must already have any ":port" suffix stripped
    pub fn route(self: *Router, hostname: []const u8) ?*Balancer {
        for (self.vhosts) |*vh| {
            for (vh.hostnames) |name| {
                if (std.ascii.eqlIgnoreCase(name, hostname)) return &vh.balancer;
            }
        }
        if (self.default) |*d| return d;
        return null;
    }
};

pub fn build(io: Io, allocator: std.mem.Allocator, config: Config) !Router {
    var vhosts = try allocator.alloc(VHost, config.vhosts.len);
    for (config.vhosts, 0..) |vc, i| {
        vhosts[i] = .{
            .hostnames = vc.hostnames,
            .balancer = try balancer.build(io, allocator, vc.backends),
        };
    }
    const default: ?Balancer = if (config.default_backends) |b|
        try balancer.build(io, allocator, b)
    else
        null;
    return .{ .vhosts = vhosts, .default = default };
}
