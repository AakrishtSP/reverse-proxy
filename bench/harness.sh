#!/usr/bin/env bash
# Benchmarks the reverse proxy across one or more configs — and optionally an
# equivalent stock-nginx proxy for comparison — with the same wrk sweep, one
# JSON row per run appended to bench/results.jsonl. Each target is named after
# its config file (basename, no extension).
#
# Usage:
#   bench/harness.sh [-c <config.zon>]... [-n] [repeats] [concurrencies...]
#
# Examples:
#   bench/harness.sh -c config.zon
#   bench/harness.sh -c bench/configs/pool-4.zon -c bench/configs/pool-16.zon
#   bench/harness.sh -c bench/configs/pool-16.zon -n 3 50 100 200 500
#
#   -c <config.zon>  proxy config to bench (repeatable). stage = basename.
#   -n               also bench an equivalent stock-nginx proxy (its own port ->
#                    the same :8888 backend) so the proxy can be compared
#                    against nginx's upstream as the http->backend hop.
#   -u               use the chunked-upload workload (bench/upload-chunked.lua)
#                    instead of GETs — exercises the chunked request-body path.
#   repeats          wrk runs per (target, concurrency). Default 1.
#   concurrency...   wrk -c values to sweep. Default 10 50 100 200 500 1000.
#
# By default wrk sends Connection: close (one-shot client connections, matching
# the no-keepalive configs). Set WRK_KEEPALIVE=1 to let wrk keep client
# connections open — required to actually exercise client/backend keep-alive
# configs (client-only, both).
#
# The backend (nginx serving "ok\n" on :8888) is started once and kept up for
# the whole sweep. Each proxy config is started on its own listen port (parsed
# from the config) and stopped after its sweep. If no -c/-n is given, defaults
# to the repo's config.zon.
#
# One-time setup (also gives the analysis notebook a matching kernel):
#   python3 -m venv .venv
#   .venv/bin/pip install ipykernel pandas matplotlib
#   .venv/bin/python -m ipykernel install --user --name reverse-proxy-bench
#
# ponytail: wrk is re-execed once per (concurrency, repeat) — no attempt to
# batch or reuse a warm process. A cold first request per run is noise on a
# 30s test; not worth engineering around.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$REPO_ROOT/bench"
RESULTS="${HARNESS_RESULTS:-$BENCH_DIR/results.jsonl}"
BIN="$REPO_ROOT/zig-out/bin/reverse_proxy"
TMP="$BENCH_DIR/tmp"
BACKEND_CONF="$TMP/backend-bench.conf"
NGINX_CONF="$TMP/nginx-proxy.conf"
BACKEND_PID="$TMP/backend.pid"
NGINX_PID="$TMP/nginx-proxy.pid"
BACKEND_PORT=8888
NGINX_PORT=8885
DURATION="${HARNESS_DURATION:-30s}"
WRK_THREADS=4
: "${WRK_ARGS:=-L}"
COMMIT="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"

CONFIGS=()
WANT_NGINX=0
UPLOAD=0
: "${WRK_KEEPALIVE:=0}"
while getopts "c:nu" opt; do
  case "${opt}" in
    c) CONFIGS+=("${OPTARG}") ;;
    n) WANT_NGINX=1 ;;
    u) UPLOAD=1 ;;
    *) echo "usage: $0 [-c <config.zon>]... [-n] [-u] [repeats] [concurrencies...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
REPEATS="${1:-1}"
shift || true
CONCURRENCIES=("${@:-10 50 100 200 500 1000}")
[ "${#CONCURRENCIES[@]}" -eq 1 ] && CONCURRENCIES=(${CONCURRENCIES[0]})

[ "${#CONFIGS[@]}" -eq 0 ] && [ "$WANT_NGINX" -eq 0 ] && CONFIGS=("$REPO_ROOT/config.zon")

[ -x "$BIN" ] || { echo "missing $BIN — run: zig build" >&2; exit 1; }
command -v nginx >/dev/null || { echo "nginx not on PATH" >&2; exit 1; }
VENV_PY="$REPO_ROOT/.venv/bin/python3"
[ -x "$VENV_PY" ] || { echo "missing $VENV_PY — run: python3 -m venv .venv && .venv/bin/pip install ipykernel pandas matplotlib" >&2; exit 1; }

mkdir -p "$TMP"

# --- parse_and_emit: wrk output file -> one JSON row -------------------------
parse_and_emit() {
    local out="$1" stage="$2" kind="$3" conn="$4" rep="$5"
    "$VENV_PY" - "$out" "$stage" "$kind" "$COMMIT" "$conn" "$WRK_THREADS" "$DURATION" "$rep" <<'PY'
import re, sys, json, datetime
text = open(sys.argv[1]).read()
def num(pat, cast=float, default=None):
    m = re.search(pat, text)
    return cast(m.group(1)) if m else default
def size_to_mb(s):
    if s is None: return None
    m = re.match(r"([\d.]+)([KMG]?)B", s)
    if not m: return None
    val, unit = float(m.group(1)), m.group(2)
    return val * {"": 1/1024/1024, "K": 1/1024, "M": 1, "G": 1024}[unit]
row = {
    "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
    "stage": sys.argv[2],
    "kind": sys.argv[3],
    "commit": sys.argv[4],
    "concurrency": int(sys.argv[5]),
    "wrk_threads": int(sys.argv[6]),
    "duration": sys.argv[7],
    "repeat": int(sys.argv[8]),
    "req_per_sec": num(r"Requests/sec:\s+([\d.]+)"),
    "transfer_per_sec_mb": size_to_mb(num(r"Transfer/sec:\s+([\d.]+[KMG]?B)", str)),
    "latency_avg_ms": num(r"Latency\s+([\d.]+)ms"),
    "latency_stdev_ms": num(r"Latency\s+[\d.]+ms\s+([\d.]+)ms"),
    "latency_max_ms": num(r"Latency\s+[\d.]+ms\s+[\d.]+ms\s+([\d.]+)ms"),
    "p50_ms": num(r"50%\s+([\d.]+)ms"),
    "p75_ms": num(r"75%\s+([\d.]+)ms"),
    "p90_ms": num(r"90%\s+([\d.]+)ms"),
    "p99_ms": num(r"99%\s+([\d.]+)ms"),
    "errors": num(r"Socket errors: connect (\d+)", int, 0),
}
print(json.dumps(row))
PY
}

run_sweep() {  # stage kind port
    local stage="$1" kind="$2" port="$3"
    for conn in "${CONCURRENCIES[@]}"; do
        for rep in $(seq 1 "$REPEATS"); do
            echo "== stage=$stage conn=$conn rep=$rep/$REPEATS ==" >&2
            local tmp; tmp="$(mktemp)"
            local -a wcmd
            if [ "$UPLOAD" -eq 1 ]; then
                # chunked-upload workload: no Connection: close, so persistence
                # follows wrk's own keep-alive.
                wcmd=(wrk -t"$WRK_THREADS" -c "$conn" -d "$DURATION" "$WRK_ARGS"
                      -s "$BENCH_DIR/upload-chunked.lua" "http://127.0.0.1:$port/")
            elif [ "$WRK_KEEPALIVE" -eq 1 ]; then
                wcmd=(wrk -t"$WRK_THREADS" -c "$conn" -d "$DURATION" "$WRK_ARGS"
                      "http://127.0.0.1:$port/")
            else
                wcmd=(wrk -t"$WRK_THREADS" -c "$conn" -d "$DURATION" "$WRK_ARGS"
                      -H "Connection: close" "http://127.0.0.1:$port/")
            fi
            if ! "${wcmd[@]}" > "$tmp" 2>&1; then
                cat "$tmp" >&2; rm -f "$tmp"; continue
            fi
            parse_and_emit "$tmp" "$stage" "$kind" "$conn" "$rep" >> "$RESULTS"
            rm -f "$tmp"
            sleep 1   # let TIME_WAIT sockets drain before the next run
        done
    done
}

wait_port() {
    local port="$1"
    for _ in $(seq 1 50); do
        (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && return 0
        sleep 0.1
    done
    return 1
}

stop_backend() { [ -f "$BACKEND_PID" ] && nginx -c "$BACKEND_CONF" -s stop 2>/dev/null || true; }
trap stop_backend EXIT

cat > "$BACKEND_CONF" <<EOF
pid $BACKEND_PID;
error_log $TMP/backend-error.log;
events { worker_connections 1024; }
http {
    access_log off;
    client_body_temp_path $TMP/backend-body;
    proxy_temp_path       $TMP/backend-proxy;
    fastcgi_temp_path     $TMP/backend-fastcgi;
    uwsgi_temp_path       $TMP/backend-uwsgi;
    scgi_temp_path        $TMP/backend-scgi;
    server { listen $BACKEND_PORT; location / { return 200 "ok\n"; } }
}
EOF
stop_backend
nginx -c "$BACKEND_CONF"
echo "backend up on :$BACKEND_PORT" >&2
sleep 0.3

count=0
for cfg in "${CONFIGS[@]}"; do
    [ -f "$cfg" ] || { echo "config not found: $cfg" >&2; exit 1; }
    [[ "$cfg" == /* ]] || cfg="$REPO_ROOT/$cfg"
    name="$(basename "$cfg" | sed 's/\.zon$//')"
    port="$(grep -oP 'listen\s*=\s*\d+' "$cfg" | grep -oP '\d+' | head -1)"
    [ -n "$port" ] || { echo "no .listen port in $cfg" >&2; exit 1; }

    echo "== starting proxy ${name} on :${port} (-c $cfg) ==" >&2
    "$BIN" -c "$cfg" > "$TMP/$name-proxy.log" 2>&1 &
    PROXY_PID=$!
    wait_port "$port" || {
        echo "proxy $name failed to listen on :$port" >&2
        cat "$TMP/$name-proxy.log" >&2
        kill "$PROXY_PID" 2>/dev/null
        exit 1
    }
    sleep 0.3
    run_sweep "$name" "reverse_proxy" "$port"
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
    count=$((count + 1))
done

if [ "$WANT_NGINX" -eq 1 ]; then
    stop_nginx_proxy() { [ -f "$NGINX_PID" ] && nginx -c "$NGINX_CONF" -s stop 2>/dev/null || true; }
    trap 'stop_nginx_proxy; stop_backend' EXIT

    cat > "$NGINX_CONF" <<EOF
pid $NGINX_PID;
error_log $TMP/nginx-proxy-error.log;
worker_processes auto;
events { worker_connections 1024; }
http {
    access_log off;
    client_body_temp_path $TMP/nginx-body;
    proxy_temp_path       $TMP/nginx-ptmp;
    fastcgi_temp_path     $TMP/nginx-fastcgi;
    uwsgi_temp_path       $TMP/nginx-uwsgi;
    scgi_temp_path        $TMP/nginx-scgi;
    server {
        listen $NGINX_PORT;
        location / { proxy_pass http://127.0.0.1:$BACKEND_PORT; }
    }
}
EOF
    stop_nginx_proxy
    nginx -c "$NGINX_CONF"
    sleep 0.3
    run_sweep "nginx" "nginx" "$NGINX_PORT"
    stop_nginx_proxy
    count=$((count + 1))
fi

echo "appended $count target(s) to $RESULTS" >&2