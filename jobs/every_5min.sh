#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${CONFIG_PATH:-config.yaml}"
LOG_DIR="${LOG_DIR:-reports/job_logs}"
PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
ALERT_COMMAND="${ALERT_COMMAND:-}"
RETRY="${RETRY:-3}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
LOG_ROTATE_BYTES="${LOG_ROTATE_BYTES:-10485760}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/every_5min.log"

rotate_job_log() {
  if [[ -f "$LOG_FILE" ]] && [[ "$(wc -c < "$LOG_FILE")" -ge "$LOG_ROTATE_BYTES" ]]; then
    mv "$LOG_FILE" "$LOG_FILE.$(date +%Y%m%d%H%M%S)"
  fi
}

run_job() {
  timeout "$TIMEOUT_SECONDS" "$PYTHON_BIN" -m oracle_pg_sync.ops sync \
    --config "$CONFIG_PATH" \
    --profile every_5min \
    --go \
    --lock-file reports/every_5min.lock \
    --log-rotate-bytes "$LOG_ROTATE_BYTES" \
    "$@"
}

rotate_job_log
set +e
status=1
for attempt in $(seq 1 "$RETRY"); do
  echo "$(date -Is) every_5min attempt=$attempt" >> "$LOG_FILE"
  run_job "$@" >> "$LOG_FILE" 2>&1
  status=$?
  [[ "$status" -eq 0 ]] && break
  sleep "$((attempt * 5))"
done
set -e
if [[ "$status" -ne 0 && -n "$ALERT_COMMAND" ]]; then
  ALERT_MESSAGE="oracle-pg-sync every_5min failed exit_code=$status log=$LOG_FILE" \
    bash -c "$ALERT_COMMAND" || true
fi
exit "$status"
