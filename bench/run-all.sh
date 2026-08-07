#!/usr/bin/env bash
# Runs the harness across every config in bench/configs/ in one go.
#
# Usage:
#   bench/run-all.sh [-n] [-u] [-k] [repeats] [concurrencies...]
#
#   -n   also bench the equivalent stock-nginx proxy (passed through to harness)
#   -u   chunked-upload workload instead of GETs (passed through to harness)
#   -k   keep-alive client workload (WRK_KEEPALIVE=1). Makes the client/both/
#        backend-only keep-alive configs meaningful; without it every request
#        is one-shot and those configs all measure the same as no-keepalive.
#   repeats, concurrency...  forwarded to the harness unchanged.
#
# Without -k, the standard Connection: close sweep runs (the no-keepalive case).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_DIR="$REPO_ROOT/bench/configs"

WANT_NGINX=0
UPLOAD=0
KEEPALIVE=0
while getopts "nuk" opt; do
  case "${opt}" in
    n) WANT_NGINX=1 ;;
    u) UPLOAD=1 ;;
    k) KEEPALIVE=1 ;;
    *) echo "usage: $0 [-n] [-u] [-k] [repeats] [concurrencies...]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

shopt -s nullglob
CONFIGS=("$CONFIG_DIR"/*.zon)
[ "${#CONFIGS[@]}" -gt 0 ] || { echo "no configs in $CONFIG_DIR" >&2; exit 1; }
echo "running ${#CONFIGS[@]} configs: $(printf '%s ' "${CONFIGS[@]##*/}")" >&2

ARGS=()
for cfg in "${CONFIGS[@]}"; do ARGS+=(-c "$cfg"); done
[ "$WANT_NGINX" -eq 1 ] && ARGS+=(-n)
[ "$UPLOAD" -eq 1 ] && ARGS+=(-u)
[ "$KEEPALIVE" -eq 1 ] && export WRK_KEEPALIVE=1

exec "$REPO_ROOT/bench/harness.sh" "${ARGS[@]}" "$@"