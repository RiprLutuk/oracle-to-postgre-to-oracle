#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config.yaml}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/reports/job_logs}"
LOCK_DIR="${LOCK_DIR:-$ROOT_DIR/reports/locks}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
ALERT_COMMAND="${ALERT_COMMAND:-}"
RETRY="${RETRY:-3}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
LOG_ROTATE_BYTES="${LOG_ROTATE_BYTES:-10485760}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-14}"

require_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer, got: $value" >&2
    exit 2
  fi
}

require_non_negative_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be a non-negative integer, got: $value" >&2
    exit 2
  fi
}

require_positive_int RETRY "$RETRY"
require_positive_int TIMEOUT_SECONDS "$TIMEOUT_SECONDS"
require_positive_int LOG_ROTATE_BYTES "$LOG_ROTATE_BYTES"
require_non_negative_int LOG_RETENTION_DAYS "$LOG_RETENTION_DAYS"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi

mkdir -p "$LOG_DIR" "$LOCK_DIR"

normalize_direction() {
  local input="${1:-}"
  case "$input" in
    oracle_to_pg|oracle-to-postgres|o2p)
      echo "oracle-to-postgres|oracle_to_pg"
      ;;
    pg_to_oracle|postgres-to-oracle|p2o)
      echo "postgres-to-oracle|pg_to_oracle"
      ;;
    *)
      echo ""
      ;;
  esac
}

job_log_file() {
  local profile="$1"
  local direction_slug="$2"
  echo "$LOG_DIR/${profile}_${direction_slug}.log"
}

job_lock_file() {
  local profile="$1"
  local direction_slug="$2"
  echo "$LOCK_DIR/${profile}_${direction_slug}.lock"
}

rotate_job_log() {
  local log_file="$1"
  if [[ -f "$log_file" ]] && [[ "$(wc -c < "$log_file")" -ge "$LOG_ROTATE_BYTES" ]]; then
    mv "$log_file" "$log_file.$(date +%Y%m%d%H%M%S)"
  fi
}

cleanup_old_logs() {
  find "$LOG_DIR" -type f -name '*.log.*' -mtime +"$LOG_RETENTION_DAYS" -delete 2>/dev/null || true
}

send_alert() {
  local message="$1"
  if [[ -n "$ALERT_COMMAND" ]]; then
    ALERT_MESSAGE="$message" bash -c "$ALERT_COMMAND" || true
  fi
}

run_sync_job() {
  local profile="$1"
  local direction="$2"
  local direction_slug="$3"
  shift 3

  local log_file
  local lock_file
  local status=1
  local attempt

  log_file="$(job_log_file "$profile" "$direction_slug")"
  lock_file="$(job_lock_file "$profile" "$direction_slug")"

  cleanup_old_logs
  rotate_job_log "$log_file"

  cd "$ROOT_DIR"
  set +e
  for attempt in $(seq 1 "$RETRY"); do
    echo "$(date -Is) profile=$profile direction=$direction attempt=$attempt" >> "$log_file"
    timeout "$TIMEOUT_SECONDS" "$PYTHON_BIN" -m oracle_pg_sync.ops sync \
      --config "$CONFIG_PATH" \
      --profile "$profile" \
      --direction "$direction" \
      --go \
      --lock-file "$lock_file" \
      --log-rotate-bytes "$LOG_ROTATE_BYTES" \
      "$@" >> "$log_file" 2>&1
    status=$?
    [[ "$status" -eq 0 ]] && break
    sleep "$((attempt * 5))"
  done
  set -e

  if [[ "$status" -ne 0 ]]; then
    send_alert "oracle-pg-sync profile=$profile direction=$direction failed exit_code=$status log=$log_file"
  fi
  return "$status"
}
