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
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-4}"
RAW_LOG_RETENTION_DAYS="${RAW_LOG_RETENTION_DAYS:-2}"
RAW_LOG_KEEP_LAST="${RAW_LOG_KEEP_LAST:-100}"

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
require_non_negative_int RAW_LOG_RETENTION_DAYS "$RAW_LOG_RETENTION_DAYS"
require_non_negative_int RAW_LOG_KEEP_LAST "$RAW_LOG_KEEP_LAST"

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

job_report_dir() {
  local profile="$1"
  echo "$ROOT_DIR/reports/cron_runs/$profile"
}

table_slug() {
  local value="$1"
  value="${value//./_}"
  value="${value//[^A-Za-z0-9_]/_}"
  printf '%s\n' "$value"
}

compact_job_runs() {
  local profile="$1"
  local output_dir="${2:-$(job_report_dir "$profile")}"

  "$PYTHON_BIN" - "$output_dir" <<'PY'
from __future__ import annotations

import csv
import json
import re
import shutil
import sys
from pathlib import Path

base = Path(sys.argv[1])
if not base.exists():
    raise SystemExit(0)

latest_root = base / "latest"
latest_root.mkdir(parents=True, exist_ok=True)
history_path = base / "run_history.csv"
fields = [
    "run_id",
    "command",
    "direction",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "tables_processed",
    "tables_failed",
    "rows_loaded",
    "source_dir",
    "latest_dir",
]

seen_run_ids: set[str] = set()
if history_path.exists():
    with history_path.open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            run_id = row.get("run_id")
            if run_id:
                seen_run_ids.add(run_id)

new_rows: list[dict[str, object]] = []
for run_dir in sorted(base.glob("run_*")):
    if not run_dir.is_dir():
        continue

    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        continue

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        continue

    command_raw = str(manifest.get("command") or "unknown")
    command = re.sub(r"[^A-Za-z0-9_.-]+", "_", command_raw).strip("._-") or "unknown"
    run_id = str(manifest.get("run_id") or run_dir.name.removeprefix("run_"))
    rows_loaded = 0
    for item in manifest.get("result_rows") or []:
        if isinstance(item, dict):
            try:
                rows_loaded += int(item.get("rows_loaded") or 0)
            except (TypeError, ValueError):
                pass

    status = "success"
    try:
        if int(manifest.get("tables_failed") or 0) > 0:
            status = "failed"
    except (TypeError, ValueError):
        status = "unknown"

    latest_dir = latest_root / command
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.move(str(run_dir), str(latest_dir))

    if run_id not in seen_run_ids:
        new_rows.append(
            {
                "run_id": run_id,
                "command": command_raw,
                "direction": manifest.get("direction", ""),
                "status": status,
                "started_at": manifest.get("started_at", ""),
                "finished_at": manifest.get("finished_at", ""),
                "duration_seconds": manifest.get("duration_seconds", ""),
                "tables_processed": manifest.get("tables_processed", ""),
                "tables_failed": manifest.get("tables_failed", ""),
                "rows_loaded": rows_loaded,
                "source_dir": run_dir.name,
                "latest_dir": str(latest_dir),
            }
        )
        seen_run_ids.add(run_id)

if new_rows:
    write_header = not history_path.exists()
    with history_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)
PY
}

make_job_config() {
  local profile="$1"
  local output_dir="${2:-$(job_report_dir "$profile")}"
  local config_out="$output_dir/config.yaml"

  mkdir -p "$output_dir"
  "$PYTHON_BIN" - "$CONFIG_PATH" "$config_out" "$output_dir" <<'PY'
from pathlib import Path
import sys
import yaml

source = Path(sys.argv[1]).resolve()
target = Path(sys.argv[2]).resolve()
output_dir = Path(sys.argv[3]).resolve()

data = yaml.safe_load(source.read_text(encoding="utf-8")) or {}

for key in ("env_file", "tables_file"):
    value = data.get(key)
    if value:
        path = Path(str(value))
        data[key] = str(path if path.is_absolute() else (source.parent / path).resolve())

sync = data.setdefault("sync", {})
checkpoint = sync.get("checkpoint_dir")
if checkpoint:
    path = Path(str(checkpoint))
    sync["checkpoint_dir"] = str(path if path.is_absolute() else (source.parent / path).resolve())

data.setdefault("reports", {})["output_dir"] = str(output_dir)

target.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
PY
  echo "$config_out"
}

rotate_job_log() {
  local log_file="$1"
  if [[ -f "$log_file" ]] && [[ "$(wc -c < "$log_file")" -ge "$LOG_ROTATE_BYTES" ]]; then
    mv "$log_file" "$log_file.$(date +%Y%m%d%H%M%S)"
  fi
}

cleanup_old_logs() {
  find "$LOG_DIR" -type f -name '*.log.*' -mtime +"$LOG_RETENTION_DAYS" -delete 2>/dev/null || true
  find "$LOG_DIR" -path '*/raw/*.log' -type f -mtime +"$RAW_LOG_RETENTION_DAYS" -delete 2>/dev/null || true
  if [[ "$RAW_LOG_KEEP_LAST" -gt 0 ]]; then
    find "$LOG_DIR" -path '*/raw/*.log' -type f -printf '%T@ %p\n' 2>/dev/null \
      | sort -rn \
      | awk -v keep="$RAW_LOG_KEEP_LAST" 'NR > keep { $1=""; sub(/^ /, ""); print }' \
      | xargs -r rm -f --
  fi
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
  local cli_profile
  local status=1
  local attempt

  log_file="$(job_log_file "$profile" "$direction_slug")"
  lock_file="$(job_lock_file "$profile" "$direction_slug")"
  cli_profile="${OPS_PROFILE:-$profile}"

  cleanup_old_logs
  rotate_job_log "$log_file"

  cd "$ROOT_DIR"
  set +e
  for attempt in $(seq 1 "$RETRY"); do
    echo "$(date -Is) profile=$profile direction=$direction attempt=$attempt" >> "$log_file"
    timeout "$TIMEOUT_SECONDS" "$PYTHON_BIN" -m oracle_pg_sync.ops sync \
      --config "$CONFIG_PATH" \
      --profile "$cli_profile" \
      --direction "$direction" \
      --go \
      --lock-file "$lock_file" \
      --log-rotate-bytes "$LOG_ROTATE_BYTES" \
      "$@" >> "$log_file" 2>&1
    status=$?
    echo "$(date -Is) profile=$profile direction=$direction attempt=$attempt exit_code=$status" >> "$log_file"
    [[ "$status" -eq 0 ]] && break
    sleep "$((attempt * 5))"
  done
  set -e

  if [[ "$status" -ne 0 ]]; then
    send_alert "oracle-pg-sync profile=$profile direction=$direction failed exit_code=$status log=$log_file"
  fi
  return "$status"
}

last_run_log_path_from_file() {
  local file="$1"
  sed -n 's/.*Run log dibuat: //p' "$file" 2>/dev/null | tail -1 || true
}

sync_result_summary_from_run_log() {
  local run_log="$1"
  local sync_result
  sync_result="$(dirname "$run_log")/sync_result.csv"

  if [[ -z "$run_log" || ! -f "$sync_result" ]]; then
    printf 'tables=0 succeeded=0 failed=0 skipped=0 rows_processed=0\n'
    return
  fi

  "$PYTHON_BIN" - "$sync_result" <<'PY'
import csv
import sys

path = sys.argv[1]
total = succeeded = failed = skipped = rows = 0
with open(path, newline="", encoding="utf-8") as fh:
    for row in csv.DictReader(fh):
        total += 1
        status = (row.get("status") or "").upper()
        if status in {"SUCCESS", "DRY_RUN", "WARNING"}:
            succeeded += 1
        elif status == "SKIPPED":
            skipped += 1
        elif status == "FAILED":
            failed += 1
        try:
            rows += int(row.get("rows_loaded") or 0)
        except ValueError:
            pass
print(f"tables={total} succeeded={succeeded} failed={failed} skipped={skipped} rows_processed={rows}")
PY
}

sequence_summary_from_log() {
  local file="$1"
  local set_count skipped_count failed_count
  set_count="$(grep -c 'Sequence set ' "$file" 2>/dev/null || true)"
  skipped_count="$(grep -c 'Sequence skip ' "$file" 2>/dev/null || true)"
  failed_count="$(grep -Ec 'Sequence .*FAILED' "$file" 2>/dev/null || true)"
  printf 'sequences_set=%s sequences_skipped=%s sequence_errors=%s\n' "$set_count" "$skipped_count" "$failed_count"
}

validation_summary_from_log() {
  local file="$1"
  "$PYTHON_BIN" - "$file" <<'PY'
import re
import sys

path = sys.argv[1]
checked = mismatch = 0
with open(path, encoding="utf-8", errors="replace") as fh:
    for line in fh:
        match = re.search(r"Rowcount .* diff=([-0-9]+)", line)
        if not match:
            continue
        checked += 1
        if int(match.group(1)) != 0:
            mismatch += 1
print(f"rowcount_checked={checked} rowcount_mismatch={mismatch}")
PY
}

strip_runtime_artifact_paths() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  sed -i \
    -e '/Run log dibuat:/d' \
    -e '/Manifest dibuat:/d' \
    "$file"
}

run_sync_job_clean() {
  local profile="$1"
  local direction="$2"
  local direction_slug="$3"
  local phase="$4"
  local log_file="$5"
  shift 5

  local lock_file
  local cli_profile
  local status=1
  local attempt
  local raw_dir
  local tmp_log
  local raw_log
  local run_log
  local summary

  lock_file="$(job_lock_file "$profile" "$direction_slug")"
  cli_profile="${OPS_PROFILE:-$profile}"
  raw_dir="$LOG_DIR/raw"
  tmp_log="$(mktemp)"

  cd "$ROOT_DIR"
  set +e
  for attempt in $(seq 1 "$RETRY"); do
    printf '%s profile=%s phase=%s attempt=%s direction=%s\n' "$(date -Is)" "$profile" "$phase" "$attempt" "$direction" >> "$tmp_log"
    timeout "$TIMEOUT_SECONDS" "$PYTHON_BIN" -m oracle_pg_sync.ops sync \
      --config "$CONFIG_PATH" \
      --profile "$cli_profile" \
      --direction "$direction" \
      --go \
      --lock-file "$lock_file" \
      --log-rotate-bytes "$LOG_ROTATE_BYTES" \
      "$@" >> "$tmp_log" 2>&1
    status=$?
    printf '%s profile=%s phase=%s attempt=%s exit_code=%s\n' "$(date -Is)" "$profile" "$phase" "$attempt" "$status" >> "$tmp_log"
    [[ "$status" -eq 0 ]] && break
    sleep "$((attempt * 5))"
  done
  set -e

  run_log="$(last_run_log_path_from_file "$tmp_log")"
  summary="$(sync_result_summary_from_run_log "$run_log")"
  if [[ "$status" -eq 0 ]]; then
    rm -f "$tmp_log"
    echo "$(date -Is) $profile phase=$phase status=COMPLETED exit_code=0 $summary" >> "$log_file"
  else
    mkdir -p "$raw_dir"
    raw_log="$raw_dir/${profile}_${phase}_$(date +%Y%m%d%H%M%S).log"
    strip_runtime_artifact_paths "$tmp_log"
    mv "$tmp_log" "$raw_log"
    echo "$(date -Is) $profile phase=$phase status=FAILED exit_code=$status $summary raw_log=$raw_log" >> "$log_file"
    send_alert "oracle-pg-sync profile=$profile phase=$phase direction=$direction failed exit_code=$status raw_log=$raw_log"
  fi
  return "$status"
}

run_command_phase_clean() {
  local profile="$1"
  local phase="$2"
  local log_file="$3"
  shift 3

  local status=1
  local raw_dir
  local tmp_log
  local raw_log
  local summary

  raw_dir="$LOG_DIR/raw"
  tmp_log="$(mktemp)"

  set +e
  timeout "$TIMEOUT_SECONDS" "$@" >> "$tmp_log" 2>&1
  status=$?
  set -e

  case "$phase" in
    sequences)
      summary="$(sequence_summary_from_log "$tmp_log")"
      ;;
    validate)
      summary="$(validation_summary_from_log "$tmp_log")"
      ;;
    *)
      summary=""
      ;;
  esac

  if [[ "$status" -eq 0 ]]; then
    rm -f "$tmp_log"
    echo "$(date -Is) $profile phase=$phase status=COMPLETED exit_code=0 $summary" >> "$log_file"
  else
    mkdir -p "$raw_dir"
    raw_log="$raw_dir/${profile}_${phase}_$(date +%Y%m%d%H%M%S).log"
    strip_runtime_artifact_paths "$tmp_log"
    mv "$tmp_log" "$raw_log"
    echo "$(date -Is) $profile phase=$phase status=FAILED exit_code=$status $summary raw_log=$raw_log" >> "$log_file"
    send_alert "oracle-pg-sync profile=$profile phase=$phase failed exit_code=$status raw_log=$raw_log"
  fi
  return "$status"
}
