# reverse-proxy

An educational HTTP/1.1 reverse proxy in Zig, with three concurrency models
built side by side sharing one HTTP/routing layer so their benchmarks are a
fair comparison:

- `threaded` — fixed pool of blocking worker threads
- `epoll` — single-threaded event loop, optionally N independent `SO_REUSEPORT`
  loops via `epoll.loops`
- `io_uring` — single-threaded ring loop (async accept/recv/send/connect)

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
            .entries = 4096,
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
`epoll.loops` / `pool_size` / `max_connections`, `io_uring.entries`). Every
kind shares `client_keepalive`, `backend_keepalive`, and `chunked_requests`
(`io_uring` serves `chunked_requests = false` for now and rejects chunked
bodies with 415). See `bench/configs/` for ready-made examples per kind.

## Backend

Benchmarks need an upstream. The bench fixture is a small C HTTP/1.1 server
(`bench/backend.c`, an epoll server answering `PONG!` on `:8888`) that is
multi-threaded — one `SO_REUSEPORT` listener per CPU — so it stops being the
throughput wall:

```bash
cc -O2 -o bench/tmp/backend bench/backend.c -lpthread
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
`concurrencies`. `run-all.sh` archives the previous `results.jsonl`
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
