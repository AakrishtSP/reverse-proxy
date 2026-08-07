#!/usr/bin/env bash
# Shared helpers for the bench scripts. Source, don't execute:
#   . "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/bench"
TMP="$BENCH_DIR/tmp"
RESULTS="${HARNESS_RESULTS:-$BENCH_DIR/results.jsonl}"
BIN="$TMP/reverse_proxy"
BACKEND_BIN="$TMP/backend"
BACKEND_PID="$TMP/backend.pid"
BACKEND_PORT=8888
COMMIT="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"

# Build the proxy ReleaseFast into $TMP, so a manual Debug `zig build` artifact
# or a stale zig-out/bin is never what gets benched.
ensure_proxy() {
    mkdir -p "$TMP"
    if ! zig build -Doptimize=ReleaseFast >/dev/null 2>&1; then
        echo "failed to build the proxy (ReleaseFast)" >&2
        exit 1
    fi
    cp -f "$REPO_ROOT/zig-out/bin/reverse_proxy" "$BIN"
    [ -x "$BIN" ] || { echo "release build produced no binary at $BIN" >&2; exit 1; }
}

# Compile the C bench backend (bench/backend.c, "PONG!" on :$BACKEND_PORT).
ensure_backend() {
    mkdir -p "$TMP"
    command -v cc >/dev/null || { echo "cc not on PATH (needed to build the C backend)" >&2; exit 1; }
    cc -O2 -o "$BACKEND_BIN" "$BENCH_DIR/backend.c" \
        || { echo "failed to build C backend" >&2; exit 1; }
}

listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

wait_port() {
    local port="$1"
    for _ in $(seq 1 50); do
        listening "$port" && return 0
        sleep 0.1
    done
    return 1
}

stop_backend() {
    [ -f "$BACKEND_PID" ] || return 0
    kill "$(cat "$BACKEND_PID")" 2>/dev/null || true
    wait "$(cat "$BACKEND_PID")" 2>/dev/null || true
    rm -f "$BACKEND_PID"
}

# Build + start the backend and wait until it accepts on :$BACKEND_PORT.
start_backend() {
    ensure_backend
    stop_backend
    "$BACKEND_BIN" > "$TMP/backend.log" 2>&1 &
    echo $! > "$BACKEND_PID"
    wait_port "$BACKEND_PORT" || {
        echo "C backend failed to listen on :$BACKEND_PORT" >&2
        cat "$TMP/backend.log" >&2
        stop_backend
        exit 1
    }
    sleep 0.3
}

config_name() { basename "$1" | sed 's/\.zon$//'; }

config_port() { grep -oP 'listen\s*=\s*\d+' "$1" | grep -oP '\d+' | head -1; }
