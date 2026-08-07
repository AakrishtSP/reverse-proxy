#!/usr/bin/env bash
# Benchmarks the reverse proxy across one or more configs — and optionally an
# equivalent stock-nginx proxy for comparison — with the same wrk sweep, one
# JSON row per run appended to bench/results.jsonl. Each target is named after
# its config file (basename, no extension).
#
# Every (target, concurrency, repeat) is benchmared under BOTH client
# workloads unless -u is given:
#   close   one-shot clients, wrk sends "Connection: close"
#   open    keep-alive clients, wrk reuses connections
# Each row is tagged with its workload in the "mode" column so the two are
# never mixed. mode=upload (from -u) replaces both.
#
# Usage:
#   bench/harness.sh [-c <config.zon>]... [-n] [-u] [repeats] [concurrencies...]
#
# Examples:
#   bench/harness.sh -c config.zon
#   bench/harness.sh -c bench/configs/10-epoll-both.zon -n 3 50 100 200
#
#   -c <config.zon>  proxy config to bench (repeatable). stage = basename.
#   -n              also bench an equivalent stock-nginx proxy (its own port ->
#                    the same :8888 backend) so the proxy can be compared
#                    against nginx's upstream as the http->backend hop. Starts
#                    with a sanity check that nginx actually proxies (curl).
#   -u              use the chunked-upload workload (bench/upload-chunked.lua)
#                    instead of GETs (mode=upload, persistent) — exercises the
#                    chunked request-body path.
#   repeats          wrk runs per (target, concurrency, mode). Default 1.
#   concurrency...   wrk -c values to sweep. Default 10 50 100 200 500 1000.
#
# The C backend (bench/backend.c, an epoll HTTP/1.1 server returning "PONG!"
# on :8888) is compiled and started once for the whole sweep. Each proxy config
# is started on its own listen port (parsed from the config) and stopped after
# its sweep. If no -c/-n is given, defaults to the repo's config.zon.
#
# Running output is a live per-(stage, mode, concurrency, repeat) line with its
# req/s; the per-stage summary line prints the mean req/s across concurrency.
#
# One-time setup (also gives the analysis notebook a matching kernel):
#   python3 -m venv .venv
#   .venv/bin/pip install ipykernel pandas matplotlib
#   .venv/bin/python -m ipykernel install --user --name reverse-proxy-bench
#
# ponytail: wrk is re-execed once per (concurrency, mode, repeat) — no attempt
# to batch or reuse a warm process. A cold first request per run is noise on a
# 30s test; not worth engineering around.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NGINX_CONF="$TMP/nginx-proxy.conf"
NGINX_PID="$TMP/nginx-proxy.pid"
NGINX_PORT=8885
DURATION="${HARNESS_DURATION:-10s}"
WRK_THREADS=4
: "${WRK_ARGS:=-L}"

CONFIGS=()
WANT_NGINX=0
UPLOAD=0
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

MODES=(close open) && [ "$UPLOAD" -eq 1 ] && MODES=(upload)

[ "${#CONFIGS[@]}" -eq 0 ] && [ "$WANT_NGINX" -eq 0 ] && CONFIGS=("$REPO_ROOT/config.zon")

mkdir -p "$TMP"
ensure_proxy
ensure_backend
command -v nginx >/dev/null 2>&1 || [ "$WANT_NGINX" -eq 0 ] || {
    echo "nginx not on PATH (needed for -n)" >&2; exit 1; }
VENV_PY="$REPO_ROOT/.venv/bin/python3"
[ -x "$VENV_PY" ] || { echo "missing $VENV_PY — run: python3 -m venv .venv && .venv/bin/pip install ipykernel pandas matplotlib" >&2; exit 1; }

# --- parse_and_emit: wrk output file -> one JSON row -------------------------
# Columns kept are exactly what the notebook consumes: identity (stage, kind,
# mode, commit, concurrency), the latency percentiles, and req/sec.
parse_and_emit() {
    local out="$1" stage="$2" kind="$3" mode="$4" conn="$5" rep="$6"
    "$VENV_PY" - "$out" "$stage" "$kind" "$mode" "$COMMIT" "$conn" "$WRK_THREADS" "$DURATION" "$rep" <<'PY'
import re, sys, json, datetime
text = open(sys.argv[1]).read()
def num(pat, cast=float, default=None):
    m = re.search(pat, text)
    return cast(m.group(1)) if m else default
# wrk prints the `-L` latency percentiles in the largest fitting unit: "us" for
# fast keep-alive runs, "ms" once latency crosses ~<1ms. Normalize everything
# to ms so a column means one thing regardless of load.
def pct_ms(pat):
    m = re.search(pat, text)
    if not m:
        return None
    val, unit = float(m.group(1)), m.group(2)
    return val * {"us": 1e-3, "ms": 1.0, "s": 1e3}.get(unit, 1.0)
row = {
    "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
    "stage": sys.argv[2],
    "kind": sys.argv[3],
    "mode": sys.argv[4],
    "commit": sys.argv[5],
    "concurrency": int(sys.argv[6]),
    "wrk_threads": int(sys.argv[7]),
    "duration": sys.argv[8],
    "repeat": int(sys.argv[9]),
    "req_per_sec": num(r"Requests/sec:\s+([\d.]+)"),
    "p50_ms": pct_ms(r"50%\s+([\d.]+)\s*(us|ms|s)\b"),
    "p75_ms": pct_ms(r"75%\s+([\d.]+)\s*(us|ms|s)\b"),
    "p90_ms": pct_ms(r"90%\s+([\d.]+)\s*(us|ms|s)\b"),
    "p99_ms": pct_ms(r"99%\s+([\d.]+)\s*(us|ms|s)\b"),
}
print(json.dumps(row))
PY
}

run_sweep() {  # stage kind port
    local stage="$1" kind="$2" port="$3"
    # per-mode running sum/sum-of-squares/count for the closing avg+stdev line
    local sum_open=0 ss_open=0 n_open=0
    local sum_close=0 ss_close=0 n_close=0
    local sum_upload=0 ss_upload=0 n_upload=0

    for conn in "${CONCURRENCIES[@]}"; do
        for wmode in "${MODES[@]}"; do
            for rep in $(seq 1 "$REPEATS"); do
                echo "== $stage mode=$wmode conn=$conn rep=$rep/$REPEATS ==" >&2
                local tmp; tmp="$(mktemp)"
                local -a wcmd
                case "$wmode" in
                    upload) wcmd=(wrk -t"$WRK_THREADS" -c "$conn" -d "$DURATION" "$WRK_ARGS"
                                  -s "$BENCH_DIR/upload-chunked.lua" "http://127.0.0.1:$port/") ;;
                    open)   wcmd=(wrk -t"$WRK_THREADS" -c "$conn" -d "$DURATION" "$WRK_ARGS"
                                  "http://127.0.0.1:$port/") ;;
                    close)  wcmd=(wrk -t"$WRK_THREADS" -c "$conn" -d "$DURATION" "$WRK_ARGS"
                                  -H "Connection: close" "http://127.0.0.1:$port/") ;;
                esac
                if ! "${wcmd[@]}" > "$tmp" 2>&1; then
                    cat "$tmp" >&2; rm -f "$tmp"; continue
                fi
                parse_and_emit "$tmp" "$stage" "$kind" "$wmode" "$conn" "$rep" | tee -a "$RESULTS"
                local rps; rps="$(grep -oP 'Requests/sec:\s+\K[\d.]+' "$tmp" | head -1)"
                printf '  %-20s mode=%-6s conn=%-4d rep=%d  ->  %s req/s\n' \
                    "$stage" "$wmode" "$conn" "$rep" "${rps:-n/a}"
                case "$wmode" in
                    open)   sum_open=$(awk "BEGIN{print $sum_open+$rps}");   ss_open=$(awk "BEGIN{print $ss_open+$rps*$rps}");   n_open=$((n_open+1)) ;;
                    close)  sum_close=$(awk "BEGIN{print $sum_close+$rps}"); ss_close=$(awk "BEGIN{print $ss_close+$rps*$rps}"); n_close=$((n_close+1)) ;;
                    upload) sum_upload=$(awk "BEGIN{print $sum_upload+$rps}"); ss_upload=$(awk "BEGIN{print $ss_upload+$rps*$rps}"); n_upload=$((n_upload+1)) ;;
                esac
                rm -f "$tmp"
                sleep 1   # let TIME_WAIT sockets drain before the next run
            done
        done
    done

    # closing summary: mean + stdev req/s per mode, so a long sweep ends with
    # a readable average instead of just the last row.
    for m in open close upload; do
        eval "local n=\$n_$m sum=\$sum_$m ss=\$ss_$m"
        [ "$n" -gt 0 ] || continue
        local mean stdev
        mean="$(awk "BEGIN{printf \"%.0f\", $sum/$n}")"
        stdev="$(awk "BEGIN{printf \"%.0f\", ($n>1)?sqrt(($ss-$sum*$sum/$n)/$n):0}")"
        printf '>> %s mode=%-6s avg %s req/s  (sd %s, %d runs)\n' "$stage" "$m" "$mean" "$stdev" "$n"
    done
}

trap stop_backend EXIT

start_backend
echo "backend up on :$BACKEND_PORT" >&2

count=0
if [ "$WANT_NGINX" -eq 1 ]; then
    # stock-nginx proxy config: own listen port -> the same :8888 backend
    write_nginx_conf() {
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
    }
    start_nginx() { write_nginx_conf; nginx -c "$NGINX_CONF"; sleep 0.3; }
    stop_nginx() { nginx -c "$NGINX_CONF" -s stop 2>/dev/null || true; }
    trap 'stop_nginx; stop_backend' EXIT

    write_nginx_conf
    nginx -t -c "$NGINX_CONF" >/dev/null 2>&1 || { echo "nginx: invalid $NGINX_CONF" >&2; exit 1; }
    # start nginx ONCE and verify it actually proxies, then leave it up for its
    # sweep at the end (nginx sits idle while the proxy configs are benched).
    start_nginx
    wait_port "$NGINX_PORT" || {
        echo "nginx failed to listen on :$NGINX_PORT" >&2
        cat "$TMP/nginx-proxy-error.log" >&2 2>/dev/null || true
        stop_nginx; stop_backend; exit 1
    }
    if [ "$(curl -s "http://127.0.0.1:$NGINX_PORT/")" != "PONG!" ]; then
        echo "nginx baseline sanity check FAILED (expected PONG! from :$NGINX_PORT)" >&2
        cat "$TMP/nginx-proxy-error.log" >&2 2>/dev/null || true
        stop_nginx; stop_backend; exit 1
    fi
    echo "nginx baseline OK (PONG! through :$NGINX_PORT)" >&2
fi

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
    run_sweep "nginx" "nginx" "$NGINX_PORT"
    stop_nginx
    count=$((count + 1))
fi

echo "appended $count target(s) to $RESULTS" >&2
