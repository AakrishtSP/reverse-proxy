# reverse-proxy

An educational HTTP/1.1 reverse proxy in Zig. Two concurrency models are
being built side by side — thread-per-connection (current) and epoll
(planned) — with the same feature set, so their benchmarks are a fair
comparison rather than an apples-to-oranges rewrite.

## Build & run

Requires Zig 0.16.0.

```bash
zig build
zig build run
```

On first run, if no config is found, you'll be offered to generate a
default `config.zon` in the current directory.

## Configure

Config is a `.zon` file, resolved in this order:

1. `$XDG_CONFIG_HOME/reverse-proxy/config.zon`
2. `~/.config/reverse-proxy/config.zon`
3. `~/.reverse-proxy/config.zon`
4. `./config.zon` (current directory)

```zon
.{
    .listen = 8080,
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
`default_backends`; no default configured means the connection is closed
with no response (nginx `return 444`-style deny).

## Benchmark

`bench.sh` spins up a throwaway nginx backend (`return 200 "ok"`) on
`:9000`, runs `wrk` against a target URL, and tears the backend down on
exit — even on Ctrl-C, via `trap`.

```bash
./bench.sh http://localhost:8080/ 30s 100 4   # url duration connections threads
```

**Port conflicts:** the backend always binds `127.0.0.1:9000`. If nginx
fails to start, something else already has that port — check with
`ss --listening --tcp src :9000` before assuming it's a stale nginx from
a previous run (`pgrep -f nginx` won't find it either way: nginx rewrites
its own process title once running, so `-f` pattern matches on the config
path stop working). `sudo lsof -i :9000` shows you the actual command
holding it. `bench/nginx.pid` only exists once nginx has successfully
bound and started — its absence means nginx never got that far, not that
cleanup is needed.

## Harness

For anything beyond a single wrk run, use `bench/harness.sh` — it sweeps
a list of concurrencies × repeats through `bench.sh`, parses each wrk
run, and appends one JSON row per run to `bench/results.jsonl` (throughput,
transfer rate, and full latency percentiles, tagged with a stage label
and the current git commit).

One-time setup (also gives the analysis notebook a matching kernel):

```bash
python3 -m venv .venv
.venv/bin/pip install ipykernel pandas matplotlib
.venv/bin/python -m ipykernel install --user --name reverse-proxy-bench
```

`bench.sh`'s `wrk` call needs `-L`/`--latency` added for percentiles to
appear in its output — not on by default.

```bash
bench/harness.sh threaded-baseline
bench/harness.sh threaded+backend-pooling 5 10 50 100 200 500 1000
```

Then open `analysis/bench_analysis.ipynb` (kernel: `reverse-proxy-bench`)
to chart throughput-vs-concurrency per stage, latency percentiles, an
improvement waterfall across stages, and — once both models exist — a
threaded-vs-epoll comparison.
