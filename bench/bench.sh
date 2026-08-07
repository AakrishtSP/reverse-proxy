#!/usr/bin/env bash
# Quick probe: builds the proxy (ReleaseFast) and the C backend, starts any
# piece not already running, and wrk's the target under BOTH client workloads
# in turn, printing each req/s. Tears its own processes down on exit.
#
#   close   one-shot clients (wrk sends "Connection: close")
#   open    keep-alive clients
#
# Usage:
#   bench/bench.sh [url] [duration] [connections] [threads]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/bench"
BACKEND_BIN="$BENCH_DIR/tmp/backend"
BACKEND_PID="$BENCH_DIR/tmp/backend.pid"
PROXY_BIN="$BENCH_DIR/tmp/reverse_proxy"
PROXY_PID="$BENCH_DIR/tmp/bench-proxy.pid"

TARGET_URL="${1:-http://localhost:8880/}"
DURATION="${2:-30s}"
CONNECTIONS="${3:-100}"
THREADS="${4:-4}"
TARGET_PORT="$(printf '%s' "$TARGET_URL" | sed -nE 's#.*:([0-9]+)/?#\1#p')"
TARGET_PORT="${TARGET_PORT:-8880}"

mkdir -p "$BENCH_DIR/tmp"
command -v cc >/dev/null || { echo "no cc on PATH (needed to build the C backend)" >&2; exit 1; }
cc -O2 -o "$BACKEND_BIN" "$BENCH_DIR/backend.c"

# ReleaseFast proxy, so this never benches a Debug build.
if ! zig build -Doptimize=ReleaseFast >/dev/null 2>&1; then
    echo "failed to build the proxy (ReleaseFast)" >&2
    exit 1
fi
cp -f "$REPO_ROOT/zig-out/bin/reverse_proxy" "$PROXY_BIN"

stop_backend() {
    [[ -f "$BACKEND_PID" ]] && kill "$(cat "$BACKEND_PID")" 2>/dev/null || true
    rm -f "$BACKEND_PID"
}
stop_proxy() {
    [[ -f "$PROXY_PID" ]] && kill "$(cat "$PROXY_PID")" 2>/dev/null || true
    rm -f "$PROXY_PID"
}
trap 'stop_proxy; stop_backend' EXIT

listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# Start whichever piece of the path isn't already running.
if ! listening 8888; then
    "$BACKEND_BIN" >"$BENCH_DIR/tmp/backend.log" 2>&1 &
    echo $! > "$BACKEND_PID"
fi
if [ "$TARGET_PORT" != 8888 ] && ! listening "$TARGET_PORT"; then
    "$PROXY_BIN" -c "$REPO_ROOT/config.zon" >"$BENCH_DIR/tmp/bench-proxy.log" 2>&1 &
    echo $! > "$PROXY_PID"
fi
sleep 0.5

for mode in open close; do
    echo "======================================================"
    echo "  mode=$mode  url=$TARGET_URL  conns=$CONNECTIONS  threads=$THREADS"
    echo "======================================================"
    if [ "$mode" = close ]; then
        wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -H "Connection: close" "$TARGET_URL"
    else
        # open is the default wrk behaviour (keep-alive)
        wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" "$TARGET_URL"
    fi
done
