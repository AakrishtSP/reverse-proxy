#!/usr/bin/env bash
# Runs the harness across every config in bench/configs/ in one go.
#
# Usage:
#   bench/run-all.sh [-n] [-u] [repeats] [concurrencies...]
#
#   -n   also bench the equivalent stock-nginx proxy (passed through to harness)
#   -u   chunked-upload workload instead of GETs (passed through to harness)
#   repeats, concurrency...  forwarded to the harness unchanged.
#
# Every config is benched under BOTH workloads (mode=close one-shot and
# mode=open keep-alive) because the harness now always runs both; see
# bench/harness.sh. The two are kept apart by each row's "mode" column.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_DIR="$REPO_ROOT/bench/configs"
RESULTS="$REPO_ROOT/bench/results.jsonl"

WANT_NGINX=0
UPLOAD=0
while getopts "nu" opt; do
  case "${opt}" in
    n) WANT_NGINX=1 ;;
    u) UPLOAD=1 ;;
    *) echo "usage: $0 [-n] [-u] [repeats] [concurrencies...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

# A full sweep is a self-contained dataset (the notebook's waterfall compares
# row-to-row), so start fresh — but archive the previous results, never delete.
if [ -s "$RESULTS" ]; then
    mv "$RESULTS" "$RESULTS.$(date +%Y%m%d-%H%M%S).bak"
    echo "archived previous results to $RESULTS.bak" >&2
fi

shopt -s nullglob
CONFIGS=("$CONFIG_DIR"/*.zon)
[ "${#CONFIGS[@]}" -gt 0 ] || { echo "no configs in $CONFIG_DIR" >&2; exit 1; }
echo "running ${#CONFIGS[@]} configs: $(printf '%s ' "${CONFIGS[@]##*/}")" >&2

ARGS=()
for cfg in "${CONFIGS[@]}"; do ARGS+=(-c "$cfg"); done
[ "$WANT_NGINX" -eq 1 ] && ARGS+=(-n)
[ "$UPLOAD" -eq 1 ] && ARGS+=(-u)

exec "$REPO_ROOT/bench/harness.sh" "${ARGS[@]}" "$@"