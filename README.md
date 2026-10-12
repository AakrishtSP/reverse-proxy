# reverse-proxy

An HTTP/1.1 reverse proxy in Zig, with three concurrency models built side
by side sharing one HTTP/routing layer so their benchmarks are a fair
comparison:

- `threaded` — fixed pool of blocking worker threads
- `epoll` — event loop, optionally N independent `SO_REUSEPORT` loops via
  `epoll.loops`
- `io_uring` — ring loop(s) with one op in flight per connection (async
  accept/recv/send/connect), optionally N independent `SO_REUSEPORT` loops
  via `io_uring.loops` with a per-loop backend connection pool

Pick one via `config.zon`'s `.server.kind`.

## Build

Requires Zig 0.16.0.

```bash
zig build                          # debug build
zig build -Doptimize=ReleaseFast   # release build (what the bench uses)
zig build test                     # run the unit tests
```

Either way the binary lands at `zig-out/bin/reverse_proxy`.

## Run

```bash
./zig-out/bin/reverse_proxy -c config.zon
```

`-c <file>` loads an explicit config. Without it the proxy looks for
`config.zon` in this order: `$XDG_CONFIG_HOME/reverse-proxy/`,
`~/.config/reverse-proxy/`, `~/.reverse-proxy/`, then `./config.zon` (current
directory). On a first run with no config present you're offered a default
`config.zon` to generate.

## Configure

```zon
.{
    .listen = 8080,
    .server = .{
        .kind = .io_uring,
        .io_uring = .{
            .client_keepalive = true,
            .loops = 8,
        },
    },
    .vhosts = .{
        .{
            .hostnames = .{"jellyfin.internal"},
            .backends = .{"127.0.0.1:8000"},
        },
    },
    .default_backends = .{"127.0.0.1:9000"},
}
```

Requests are routed by the `Host` header against `vhosts[].hostnames`
(case-insensitive, port suffix ignored). No match falls through to
`default_backends`; no default configured means the connection is closed with
no response (nginx `return 444`-style deny).

`.server` holds a `kind` plus per-kind options (`threaded.pool_threads`,
`epoll.loops` / `pool_size` / `max_connections`, `io_uring.loops` /
`pool_size` / `max_connections` / `sqpoll` / `entries`). Every kind shares
`client_keepalive`, `backend_keepalive`, and `chunked_requests`
(`io_uring` serves `chunked_requests = false` for now and rejects chunked
bodies with 415). See `bench/configs/` for ready-made examples per kind
(`22-io-uring-loops-8` is the fast io_uring setup: pooled backends × 8
loops).

## Features

**What this is:** an HTTP/1.1 reverse proxy written to compare three
concurrency models (thread pool, epoll loops, io_uring loops) on equal
footing — same HTTP parsing, routing, and bench harness for all three.

**What it has:**

- Host-based routing: `vhosts[].hostnames` matched case-insensitively
  against the `Host` header (port suffix stripped), `default_backends`
  fallback, silent deny when nothing matches (nginx `return 444`-style).
- Keep-alive on both hops: client keep-alive (HTTP/1.1 default,
  HTTP/1.0 explicit) and backend connection reuse — per-client in
  `threaded`, shared pool in `epoll` and `io_uring` (with reconnect-once
  for backend-idle-closed sockets).
- Body relay at any size: `Content-Length` streams through fixed buffers;
  chunked request/response bodies stream through a framing scanner without
  ever decoding or buffering them (`threaded`, `epoll`; io_uring relays
  chunked *responses* and rejects chunked *requests* with 415).
- Correct framing: 1xx/204/304 and HEAD responses end at the head
  (`threaded`, `epoll`); close-delimited responses relay to EOF and are
  never reused.
- Proxy hygiene: hop-by-hop headers stripped (including anything named by
  `Connection`), `Connection` rewritten per hop, client-supplied
  `X-Forwarded-For` replaced (not chained) so it can't be spoofed, `Via`
  added (`threaded`, `epoll`).
- Smuggling guards: duplicate/conflicting `Content-Length`, `CL` +
  `Transfer-Encoding` together, whitespace before the header colon, and
  over-long heads are all rejected (400/431/415).
- Dual-stack listeners (v4 arrives v4-mapped) and v4/v6 backends, 8KB
  request-head cap (431 beyond it), round-robin balancing with DNS
  resolution at startup, ZON config with search paths, unit-tested
  parser/chunked framing, and a full bench harness (wrk sweeps → JSONL →
  notebooks).

**What it does not have (by design, not by accident):**

- No TLS, no HTTP/2 — plain HTTP/1.1 only.
- No timeouts anywhere: no connect, read, write, or request deadlines.
- No access log (errors go to stderr via a non-blocking background
  logger); no `/stats` or metrics endpoint.
- No backend health checks, no failover/retries (a dead backend is a
  502 or a closed connection, not a reroute), no graceful shutdown.
- Round-robin balancing only: no weights, no least-connections, no
  hashing.
- No caching, compression, rate limiting, authentication, or request
  filtering.
- io_uring gaps: rejects chunked request bodies (415), and has no
  bodyless-response handling (a `HEAD` request stalls waiting for body
  bytes the backend correctly never sends).

**Educational or production-ready? Educational.** It is a performance
laboratory: three interchangeable engines behind one test rig, built to
measure rather than to serve. The missing timeouts alone disqualify it
for production (any client can hold connections and buffers forever —
classic Slowloris shape), and operability (logs, health, shutdown,
failover) was never built. Take the measurements, the parser, and the
framing scanner; do not put this on the open internet.

## Backend

Benchmarks need an upstream. The bench fixture is a small C HTTP/1.1 server
(`bench/backend.c`, an io_uring multishot server answering `PONG!` on
`:8888`) that is multi-threaded — one `SO_REUSEPORT` listener per CPU — so
it stops being the throughput wall:

```bash
cc -O2 -o bench/tmp/backend bench/backend.c -luring -lpthread
./bench/tmp/backend
```

The bench scripts below compile and run it for you; this is only needed for
manual testing.

## Benchmark

`bench/bench.sh` builds the proxy (ReleaseFast) and the C backend, starts
anything not already running, then wrk's the target under **both** client
workloads in turn and prints each req/s, tearing its own processes down on
exit (even on Ctrl-C):

```bash
bench/bench.sh                              # defaults: http://localhost:8880/ 30s 100 conns 4 threads
bench/bench.sh http://localhost:8880/ 10s 50 4
```

Workloads: `close` = one-shot clients (`wrk` sends `Connection: close`),
`open` = keep-alive clients.

## Harness

For sweeps, `bench/harness.sh` runs every requested config at each of a list
of concurrencies under both workloads and appends one JSON row per run to
`bench/results.jsonl` (throughput, transfer rate, full latency percentiles,
tagged with the config's stage name and current git commit). The C backend is
compiled and started once for the whole sweep; each config is started on its
own port and stopped after its run.

```bash
bench/harness.sh                                   # default: repo config.zon, c 10 50 100 200 500 1000
bench/harness.sh -c bench/configs/14-epoll-loops-4.zon -n 3 50 200
bench/run-all.sh                                   # every config in bench/configs/ (fresh, archives old)
bench/run-missing.sh                               # ONLY configs with no results rows yet (append-only)
```

Flags: `-c <config.zon>` (repeatable, stage = basename), `-n` (also bench an
equivalent stock-nginx proxy against the same `:8888` backend), `-u`
(chunked-upload workload via `bench/upload-chunked.lua`), then `repeats` and
`concurrencies`. Environment overrides: `WRK_THREADS` (wrk threads, default
4 — 8 drives more past ~6M req/s on 16 cores), `HARNESS_DURATION` (default
`10s`), `HARNESS_RESULTS` (redirect rows elsewhere instead of
`bench/results.jsonl`). `run-all.sh` archives the previous `results.jsonl`
(`.bak`) and starts fresh; `run-missing.sh` (add `-a` to force every config)
appends only the configs that aren't in `results.jsonl` yet, so after adding
one config you bench just that one instead of the whole set.

`bench/lib.sh` is the shared core the bench scripts all source (ReleaseFast
proxy build, C backend build/start/stop, port probes, config helpers); the
entrypoints only keep their own orchestration.

One-time setup for the analysis notebook:

```bash
python3 -m venv .venv
.venv/bin/pip install ipykernel pandas matplotlib
.venv/bin/python -m ipykernel install --user --name reverse-proxy-bench
```

Then open `analysis/bench_analysis.ipynb` (kernel: `reverse-proxy-bench`) to
chart throughput-vs-concurrency per stage, latency percentiles, an improvement
waterfall across stages, and the threaded-vs-epoll-vs-io_uring comparison.

## Results

Machine: AMD Ryzen 7 260 (16 cores), Linux 7.2, `performance` governor,
proxy + C backend + wrk all on the same box, ReleaseFast. Best req/s per
stage across concurrencies 10–1000 (`bench/results.jsonl`); `close` =
one-shot clients, `open` = keep-alive clients.

| stage | kind | close | open |
|---|---|---:|---:|
| `00-no-keepalive` | threaded, nothing reused | 16,190 | 15,854 |
| `01-client-only` | threaded, client keep-alive | 15,963 | 16,255 |
| `02-backend-only` | threaded, backend keep-alive | 15,683 | 15,475 |
| `03-both` | threaded, keep-alive both hops | 15,883 | 157,789 |
| `04-chunked` | threaded + chunked bodies | 16,132 | 160,024 |
| `05-pool-16` | threaded, 16 workers | 15,918 | 161,891 |
| `06-pool-4` | threaded, 4 workers | 14,512 | 91,835 |
| `07-epoll-no-keepalive` | epoll, nothing reused | 12,875 | 12,994 |
| `08-epoll-client-only` | epoll, client keep-alive | 12,960 | 13,931 |
| `09-epoll-backend-only` | epoll, backend keep-alive | 22,038 | 22,582 |
| `10-epoll-both` | epoll, keep-alive both hops | 22,762 | 35,906 |
| `11-epoll-chunked` | epoll + chunked bodies | 22,450 | 35,834 |
| `12-epoll-pool-100` | epoll, backend pool cap 100 | 22,370 | 36,176 |
| `13-epoll-pool-4` | epoll, backend pool cap 4 | 17,693 | 32,361 |
| `16-epoll-loops-2` | epoll × 2 loops | 39,728 | 71,022 |
| `14-epoll-loops-4` | epoll × 4 loops | 69,287 | 134,143 |
| `17-epoll-loops-8` | epoll × 8 loops | 70,845 | 220,556 |
| `18-io-uring-no-keepalive` | io_uring, nothing reused | 27,106 | 27,084 |
| `19-io-uring-client-only` | io_uring, client keep-alive | 14,247 | 14,415 |
| `15-io-uring` | io_uring, keep-alive both hops | 26,502 | 123,366 |
| `20-io-uring-entries-1024` | io_uring, smaller ring | 26,902 | 122,325 |
| `21-io-uring-entries-16384` | io_uring, bigger ring | 26,838 | 123,767 |
| `22-io-uring-loops-8` | io_uring, pooled backends × 8 loops | 124,546 | 526,995 |
| `23-io-uring-loops-8-sqpoll` | as 22 + kernel poll thread | 84,662 | 393,575 |

Older stages were benched with `wrk -t4`; the io_uring stages with `-t8`
(recorded per row in `wrk_threads`). Takeaways:

- **Fastest is io_uring with pooled backends × 8 loops** (`22`):
  ~527k open / ~125k close. Single-loop io_uring (`15`) does ~123k /
  ~27k, so multi-loop is worth ~4× — one core with one in-flight op per
  connection saturates early.
- **Keep-alive on both hops is everything.** Threaded jumps 16k → 158k
  open (`00` → `03`); epoll jumps 13k → 36k open (`07` → `10`); io_uring
  jumps 27k → 123k open (`18` → `15`). Without backend reuse every request
  pays a fresh TCP handshake upstream.
- **epoll loops scale near-linearly while cores last:** open 36k → 71k →
  134k → 221k for 1 → 2 → 4 → 8 loops.
- **`close` is an order of magnitude below `open` everywhere.** `open`
  (keep-alive) is CPU-bound; `close` is connection-setup-bound
  (handshake + accept per request). "Which server is faster" flips with the
  mode, so both are always reported.
- **Ring depth doesn't matter** (`20` vs `21` vs `15` are identical):
  in-flight depth per loop is a handful of ops; depth only costs pinned
  memory (`RLIMIT_MEMLOCK` is shared between proxy and backend rings).
- **SQPOLL loses** (`23` trails `22` almost everywhere, badly at low
  concurrency): 8 kernel poll threads add wakeup latency when lightly
  loaded and oversubscribe when busy. It stays an opt-in flag.
- **Throughput sags past c500 on every server**, including stock
  threaded/epoll rows from before any tuning — systemic to the box
  (thermal soak over long sweeps and same-box `wrk`), not the code.
