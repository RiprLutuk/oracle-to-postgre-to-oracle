#!/usr/bin/env bash
set -euo pipefail

# Copy this template and keep table-specific reverse keys/filters in the final arguments.
APP_DIR="${APP_DIR:-/home/app/oracle-pg-sync-audit}"
CONFIG_PATH="${CONFIG_PATH:-$APP_DIR/config.yaml}"
RETRY="${RETRY:-3}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
ALERT_COMMAND="${ALERT_COMMAND:-echo FAILED}"

cd "$APP_DIR"
RETRY="$RETRY" \
TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
ALERT_COMMAND="$ALERT_COMMAND" \
CONFIG_PATH="$CONFIG_PATH" \
  jobs/every_5min.sh \
  --direction postgres-to-oracle \
  --tables public.address \
  --mode upsert \
  --key-columns address_id \
  --incremental-column last_update
