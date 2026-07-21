#!/usr/bin/env bash
# Runs bench.sh across a concurrency sweep, N repeats each, parses wrk's
# output, appends one JSON object per line to bench/results.jsonl.
#
# Usage: bench/harness.sh <stage-label> [repeats] [concurrencies...]
# Example:
#   bench/harness.sh threaded-baseline
#   bench/harness.sh threaded+bigger-pump-buffer 5 10 50 100 200 500 1000
#
# One-time setup (also gives the analysis notebook a matching kernel):
#   python3 -m venv .venv
#   .venv/bin/pip install ipykernel pandas matplotlib
#   .venv/bin/python -m ipykernel install --user --name reverse-proxy-bench
# Then in bench_analysis.ipynb: Kernel -> Change Kernel -> reverse-proxy-bench
#
# ponytail: wrk is re-execed once per (concurrency, repeat) — no attempt to
# batch or reuse a warm process. A cold first request per run is noise on a
# 30s test; not worth engineering around.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="$REPO_ROOT/bench/results.jsonl"

STAGE="${1:?usage: harness.sh <stage-label> [repeats] [concurrencies...]}"
REPEATS="${2:-3}"
shift $(( $# >= 2 ? 2 : 1 ))
CONCURRENCIES=("${@:-10 50 100 200 500 1000}")
[ "${#CONCURRENCIES[@]}" -eq 1 ] && CONCURRENCIES=(${CONCURRENCIES[0]})

COMMIT="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DURATION="30s"
WRK_THREADS=4
TARGET_URL="http://localhost:8880/"

# wrk needs -L/--latency to emit the percentile table bench.sh doesn't ask
# for by default. Set WRK_ARGS="-L" in your shell, or edit bench.sh's wrk
# invocation to always include it — one line, not duplicated here.
: "${WRK_ARGS:=-L}"

VENV_PY="$REPO_ROOT/.venv/bin/python3"
[ -x "$VENV_PY" ] || { echo "missing $VENV_PY — run: python3 -m venv .venv && .venv/bin/pip install ipykernel pandas matplotlib" >&2; exit 1; }

parse_and_emit() {
    local out="$1" conn="$2" rep="$3"
    "$VENV_PY" - "$out" "$STAGE" "$COMMIT" "$conn" "$WRK_THREADS" "$DURATION" "$rep" <<'PY'
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
    "commit": sys.argv[3],
    "concurrency": int(sys.argv[4]),
    "wrk_threads": int(sys.argv[5]),
    "duration": sys.argv[6],
    "repeat": int(sys.argv[7]),
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

mkdir -p "$REPO_ROOT/bench"
for conn in "${CONCURRENCIES[@]}"; do
    for rep in $(seq 1 "$REPEATS"); do
        echo "== stage=$STAGE conn=$conn rep=$rep/$REPEATS ==" >&2
        tmp="$(mktemp)"
        WRK_ARGS="$WRK_ARGS" "$REPO_ROOT/bench/bench.sh" "$TARGET_URL" "$DURATION" "$conn" "$WRK_THREADS" \
            > "$tmp" 2>&1 || { cat "$tmp" >&2; rm -f "$tmp"; continue; }
        parse_and_emit "$tmp" "$conn" "$rep" >> "$RESULTS"
        rm -f "$tmp"
        sleep 1  # let TIME_WAIT sockets from this run drain before the next
    done
done

echo "appended to $RESULTS" >&2
