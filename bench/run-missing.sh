#!/usr/bin/env bash
# Runs the harness only on configs in bench/configs/ that have no rows yet in
# bench/results.jsonl — append-only, never archives. Perfect after adding a
# new config: it benches just the new ones instead of the whole set (run-all
# takes several minutes).
#
# Usage:
#   bench/run-missing.sh [-n] [-u] [-a] [repeats] [concurrencies...]
#
#   -a   ignore "already benched": run every config (same as bench/run-all.sh)
#   -n   also bench the equivalent stock-nginx proxy (passed through)
#   -u   chunked-upload workload (passed through)
#   repeats, concurrency...  forwarded to the harness unchanged.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

CONFIG_DIR="$BENCH_DIR/configs"

WANT_NGINX=0
UPLOAD=0
FORCE=0
while getopts "nua" opt; do
  case "${opt}" in
    n) WANT_NGINX=1 ;;
    u) UPLOAD=1 ;;
    a) FORCE=1 ;;
    *) echo "usage: $0 [-n] [-u] [-a] [repeats] [concurrencies...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

shopt -s nullglob
ALL=("$CONFIG_DIR"/*.zon)
[ "${#ALL[@]}" -gt 0 ] || { echo "no configs in $CONFIG_DIR" >&2; exit 1; }

MISSING=()
if [ "$FORCE" -eq 1 ] || [ ! -s "$RESULTS" ]; then
    MISSING=("${ALL[@]}")
else
    for cfg in "${ALL[@]}"; do
        stage="$(config_name "$cfg")"
        grep -q "\"stage\": \"$stage\"" "$RESULTS" || MISSING+=("$cfg")
    done
fi

[ "${#MISSING[@]}" -gt 0 ] || {
    echo "nothing missing: all ${#ALL[@]} configs already have rows in $RESULTS" >&2
    exit 0
}
echo "benching ${#MISSING[@]} of ${#ALL[@]} configs: $(printf '%s ' "${MISSING[@]##*/}")" >&2

ARGS=()
for cfg in "${MISSING[@]}"; do ARGS+=(-c "$cfg"); done
[ "$WANT_NGINX" -eq 1 ] && ARGS+=(-n)
[ "$UPLOAD" -eq 1 ] && ARGS+=(-u)

exec "$BENCH_DIR/harness.sh" "${ARGS[@]}" "$@"