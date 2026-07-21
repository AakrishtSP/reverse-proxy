#!/usr/bin/env bash
# Starts the nginx bench backend, runs wrk against it (or whatever's passed),
# tears the backend down after — even on failure/Ctrl-C, via trap.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/bench"
CONF="$BENCH_DIR/backend-bench.conf"
PIDFILE="$BENCH_DIR/nginx.pid"

TARGET_URL="${1:-http://localhost:8880/}"
DURATION="${2:-30s}"
CONNECTIONS="${3:-100}"
THREADS="${4:-4}"

mkdir -p "$BENCH_DIR/tmp"

cat > "$CONF" <<EOF
pid $PIDFILE;
error_log $BENCH_DIR/error.log;

events { worker_connections 1024; }

http {
    access_log off;
    client_body_temp_path $BENCH_DIR/tmp/client_body;
    proxy_temp_path       $BENCH_DIR/tmp/proxy;
    fastcgi_temp_path     $BENCH_DIR/tmp/fastcgi;
    uwsgi_temp_path       $BENCH_DIR/tmp/uwsgi;
    scgi_temp_path        $BENCH_DIR/tmp/scgi;

    server {
        listen 8888;
        location / {
            return 200 "ok\n";
        }
    }
}
EOF

stop_backend() {
    if [[ -f "$PIDFILE" ]]; then
        nginx -c "$CONF" -s stop 2>/dev/null || true
        rm -f "$PIDFILE"
    fi
}
trap stop_backend EXIT

stop_backend  # in case a stale instance is already running from a previous crash
nginx -c "$CONF"

# nginx forks and returns almost immediately, but give it a moment before hammering it
sleep 0.3

wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -H "Connection: close" "$TARGET_URL"
