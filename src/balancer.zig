const std = @import("std");
const Io = std.Io;

pub const Balancer = struct {
    backends: []const Io.net.IpAddress,
    next: std.atomic.Value(usize) = .init(0),

    pub fn pick(self: *Balancer) Io.net.IpAddress {
        const i = self.next.fetchAdd(1, .monotonic) % self.backends.len;
        return self.backends[i];
    }
};

/// Resolves one "host:port", "[ipv6]:port", or "hostname:port" entry.
/// Literals are free (no syscall). Hostnames pay one DNS lookup.
fn resolveOne(io: Io, addr: []const u8) !Io.net.IpAddress {
    if (Io.net.IpAddress.parseLiteral(addr)) |ip| return ip else |_| {}

    const colon = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidAddress;
    const host = try Io.net.HostName.init(addr[0..colon]);
    const port = try std.fmt.parseInt(u16, addr[colon + 1 ..], 10);

    var buf: [8]Io.net.HostName.LookupResult = undefined;
    var results: Io.Queue(Io.net.HostName.LookupResult) = .init(&buf);

    var future = io.async(Io.net.HostName.lookup, .{ host, io, &results, .{ .port = port } });

    const first = try results.getOne(io);
    try future.await(io);
    return first.address;
}

fn resolveAll(io: Io, allocator: std.mem.Allocator, addrs: []const []const u8) ![]Io.net.IpAddress {
    var out = try allocator.alloc(Io.net.IpAddress, addrs.len);
    for (addrs, 0..) |a, i| out[i] = try resolveOne(io, a);
    return out;
}

pub fn build(io: Io, allocator: std.mem.Allocator, addrs: []const []const u8) !Balancer {
    return .{ .backends = try resolveAll(io, allocator, addrs) };
}
