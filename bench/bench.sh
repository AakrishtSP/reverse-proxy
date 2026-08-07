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
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PROXY_BIN="$BIN"
PROXY_PID="$TMP/bench-proxy.pid"

TARGET_URL="${1:-http://localhost:8880/}"
DURATION="${2:-30s}"
CONNECTIONS="${3:-100}"
THREADS="${4:-4}"
TARGET_PORT="$(printf '%s' "$TARGET_URL" | sed -nE 's#.*:([0-9]+)/?#\1#p')"
TARGET_PORT="${TARGET_PORT:-8880}"

ensure_proxy
ensure_backend

stop_proxy() {
    [[ -f "$PROXY_PID" ]] && kill "$(cat "$PROXY_PID")" 2>/dev/null || true
    rm -f "$PROXY_PID"
}
trap 'stop_proxy; stop_backend' EXIT

# Start whichever piece of the path isn't already running.
if ! listening "$BACKEND_PORT"; then
    "$BACKEND_BIN" >"$TMP/backend.log" 2>&1 &
    echo $! > "$BACKEND_PID"
fi
if [ "$TARGET_PORT" != "$BACKEND_PORT" ] && ! listening "$TARGET_PORT"; then
    "$PROXY_BIN" -c "$REPO_ROOT/config.zon" >"$TMP/bench-proxy.log" 2>&1 &
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