#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Keep this higher than the heaviest table refresh.
export TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-14400}"
export RETRY="${RETRY:-1}"
export SEQUENCE_BUFFER="${SEQUENCE_BUFFER:-1000}"

source "$SCRIPT_DIR/common.sh"

MODE="${MODE:-truncate}"
DIRECTION_INPUT="${1:-oracle_to_pg}"
if [[ $# -gt 0 ]]; then
  shift
fi

direction_pair="$(normalize_direction "$DIRECTION_INPUT")"
if [[ -z "$direction_pair" || "${direction_pair%%|*}" != "oracle-to-postgres" ]]; then
  echo "production_keepup.sh only supports oracle_to_pg/oracle-to-postgres" >&2
  exit 2
fi

direction="${direction_pair%%|*}"
direction_slug="${direction_pair##*|}"

sync_args=(--mode "$MODE" --skip-dependencies)
validate_args=(--direction "$direction")
sequence_args=()
table_scope="config:${CONFIG_PATH}"

if [[ -n "${TABLES:-}" ]]; then
  # Example: TABLES="public.table_a public.table_b"
  read -r -a selected_tables <<< "$TABLES"
  sync_args+=(--tables "${selected_tables[@]}")
  validate_args+=(--tables "${selected_tables[@]}")
  sequence_args+=(--tables "${selected_tables[@]}")
  table_scope="override:${#selected_tables[@]}"
fi

log_file="$(job_log_file "daily" "$direction_slug")"

echo "$(date -Is) production_keepup start tables=$table_scope mode=$MODE" >> "$log_file"
run_sync_job daily "$direction" "$direction_slug" "${sync_args[@]}" "$@"

echo "$(date -Is) production_keepup sync PostgreSQL sequences from Oracle" >> "$log_file"
set +e
timeout "$TIMEOUT_SECONDS" "$PYTHON_BIN" -m oracle_pg_sync.ops sync-sequences \
  --config "$CONFIG_PATH" \
  --go \
  --sequence-source oracle-list \
  --sequence-buffer "$SEQUENCE_BUFFER" \
  "${sequence_args[@]}" >> "$log_file" 2>&1
sequence_status=$?
set -e

if [[ "$sequence_status" -ne 0 ]]; then
  send_alert "oracle-pg-sync production_keepup sequence sync failed exit_code=$sequence_status log=$log_file"
  exit "$sequence_status"
fi

echo "$(date -Is) production_keepup validate exact rowcount" >> "$log_file"
set +e
timeout "$TIMEOUT_SECONDS" "$PYTHON_BIN" -m oracle_pg_sync.ops validate \
  --config "$CONFIG_PATH" \
  "${validate_args[@]}" >> "$log_file" 2>&1
validate_status=$?
set -e

if [[ "$validate_status" -ne 0 ]]; then
  send_alert "oracle-pg-sync production_keepup validation mismatch exit_code=$validate_status log=$log_file"
  exit "$validate_status"
fi

echo "$(date -Is) production_keepup done" >> "$log_file"
