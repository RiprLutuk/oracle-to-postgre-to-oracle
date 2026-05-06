#!/usr/bin/env bash
set -euo pipefail

# Copy this template outside the repository if your scheduler manages code deploys separately.
APP_DIR="${APP_DIR:-/home/app/oracle-pg-sync-audit}"
CONFIG_PATH="${CONFIG_PATH:-$APP_DIR/config.yaml}"
SYNC_DIRECTION="${SYNC_DIRECTION:-oracle_to_pg}"
RETRY="${RETRY:-3}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
ALERT_COMMAND="${ALERT_COMMAND:-printf '%s\n' \"\$ALERT_MESSAGE\"}"

cd "$APP_DIR"
RETRY="$RETRY" \
TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
ALERT_COMMAND="$ALERT_COMMAND" \
CONFIG_PATH="$CONFIG_PATH" \
  jobs/daily.sh "$SYNC_DIRECTION"
