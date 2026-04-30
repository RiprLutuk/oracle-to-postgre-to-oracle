#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: jobs/incremental.sh oracle_to_pg|pg_to_oracle [sync args...]" >&2
  exit 2
fi

direction_pair="$(normalize_direction "$1")"
if [[ -z "$direction_pair" ]]; then
  echo "Unsupported direction: $1" >&2
  exit 2
fi
shift

direction="${direction_pair%%|*}"
direction_slug="${direction_pair##*|}"
run_sync_job every_5min "$direction" "$direction_slug" "$@"
