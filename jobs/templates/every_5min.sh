#!/usr/bin/env bash
set -euo pipefail

# Copy this template and keep table-specific reverse keys/filters in the final arguments.
APP_DIR="${APP_DIR:-/home/app/oracle-pg-sync-audit}"
CONFIG_PATH="${CONFIG_PATH:-$APP_DIR/config.yaml}"
SYNC_DIRECTION="${SYNC_DIRECTION:-pg_to_oracle}"
RETRY="${RETRY:-3}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
ALERT_COMMAND="${ALERT_COMMAND:-printf '%s\n' \"\$ALERT_MESSAGE\"}"

cd "$APP_DIR"
RETRY="$RETRY" \
TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
ALERT_COMMAND="$ALERT_COMMAND" \
CONFIG_PATH="$CONFIG_PATH" \
  jobs/incremental.sh \
  "$SYNC_DIRECTION" \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at
