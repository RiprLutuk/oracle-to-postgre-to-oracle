# Production Runbook

## 1. Prepare The Host

1. Install Python 3.11+.
2. Install Oracle client libraries if thick mode is required.
3. Create `.venv` and install `pip install -e ".[dev]"`.
4. Place `.env`, `config.yaml`, and `configs/tables.yaml` outside source control if possible.
5. Confirm the service account can reach both databases.

## 2. Validate Before First Execute

Run:

```bash
ops doctor --config config.yaml
ops audit --config config.yaml
ops analyze lob --config config.yaml
ops dependencies check --config config.yaml
```

Review:

- `report.html`
- `report.xlsx`
- schema diff `ERROR` rows
- dependency `broken_count`
- LOB-heavy recommendations

Do not start execute mode until the audit is clean or the exceptions are understood.

For a single table such as `A_HP_BATCH`, confirm table resolution and counts before the first load:

```bash
ops audit --config config.yaml --tables A_HP_BATCH --exact-count
ops validate --config config.yaml --tables A_HP_BATCH
```

## 3. First Oracle -> PostgreSQL Execute

1. Dry-run:

```bash
ops sync --config config.yaml --direction oracle-to-postgres
```

2. Risk simulation:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --simulate
```

3. Execute:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --profile daily --go
```

Direct truncate is available when you intentionally want the fast destructive path:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables A_HP_BATCH --lob include --mode truncate --go
```

Use `truncate_safe` instead when staging must be validated before the live target is changed:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables A_HP_BATCH --lob include --mode truncate_safe --go
```

4. Review:

- `sync_result.csv`
- `dependency_summary.csv`
- `validation_checksum.csv` if enabled
- `metrics.json`
- `manifest.json`

Confirm these fields for every table:

- `rows_read_from_oracle`
- `rows_written_to_postgres`
- `rows_failed`
- `row_count_match`
- `row_count_diff`
- `validation_status`

## 4. First PostgreSQL -> Oracle Execute

Prefer `upsert` with keys for reverse sync. Reverse safe-mode rollback is not
as complete as Oracle -> PostgreSQL safe modes, so use Oracle-native backup or
a DBA-approved restore path for destructive reverse full refreshes.

1. Dry-run:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at \
  --incremental
```

2. Execute:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at \
  --incremental \
  --go
```

3. Confirm:

- `status` is `SUCCESS` or an expected `WARNING`
- no checksum mismatch
- `data_integrity_status` is `PASS` when validation scope is complete
- no critical dependency failures after the run
- `ops validate --direction postgres-to-oracle --tables public.sample_customer` is clean
- `ops validate missing-keys --direction postgres-to-oracle --tables public.sample_customer` is clean when keys are configured

For reverse full replace, use only during a maintenance window:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.table_name --mode truncate --go
```

## 5. Daily Operations

Recommended operator commands:

```bash
ops status --config config.yaml
ops report latest --config config.yaml
ops circuit-breaker list --config config.yaml
ops dependencies check --config config.yaml
```

Use the run folder as the unit of investigation. Do not mix files from different run directories.
For detailed daily DBA command flow, validation SOP, rollback SOP, and sync mode
decision matrix, use [DBA Daily Operations Guide](DBA_DAILY_OPERATIONS.md).

## 6. Cron Deployment

Full refresh:

```bash
jobs/daily.sh oracle_to_pg
jobs/daily.sh pg_to_oracle
```

Incremental:

```bash
jobs/incremental.sh oracle_to_pg
jobs/incremental.sh pg_to_oracle --tables public.sample_customer --mode upsert --key-columns customer_id --incremental-column updated_at
```

Job wrapper guarantees:

- one lock file per profile and direction
- retry loop
- timeout
- circuit breaker after repeated failures
- structured webhook/email alert hook
- log rotation
- old rotated log cleanup

## 7. Failure Recovery

### Sync Failed

1. Check `reports/run_<...>/logs.txt`.
2. Open `report.xlsx` and `report.html`.
3. Check whether the run created rollback metadata in `manifest.json` and `rollback_result.csv`.
4. If the failure is transient and checkpointable, run:

```bash
ops resume --config config.yaml
```

5. If the incremental watermark is wrong, inspect and reset:

```bash
ops watermarks --config config.yaml
ops reset-watermark public.sample_customer --config config.yaml
```

6. If the run reached dependency failure after data cutover, restore the last safe backup:

```bash
ops rollback <run_id> --config config.yaml
```

### Dependency Failure

1. Review `dependency_pre.csv`, `dependency_post.csv`, and `dependency_summary.csv`.
2. Run:

```bash
ops dependencies repair --config config.yaml
```

3. If repair still exits non-zero, the execute run must stay failed. Use `ops rollback <run_id>` if the run already cut over data, then escalate to DBA review.

### Checksum Failure

Treat checksum mismatch as a hard validation failure. Do not mark the run successful until source/target row selection, keying, and LOB policy are verified.

By default checksum excludes `BLOB`, `CLOB`, `NCLOB`, `LONG`, `LONG RAW`, and `bytea` columns. Validate LOBs separately by size or explicit hash when needed.

### Rowcount Mismatch

A rowcount mismatch makes the table fail when `validation.rowcount.fail_on_mismatch: true`. No watermark is updated for failed runs.

1. Check `source_schema`, `source_table`, `target_schema`, `target_table`, and `effective_where` in `sync_result.csv`.
2. Re-run exact validation:

```bash
ops validate --config config.yaml --tables A_HP_BATCH
```

3. If keys are configured, produce missing/extra samples:

```bash
ops validate missing-keys --config config.yaml --tables A_HP_BATCH
```

Review `keys_in_oracle_not_in_postgres.csv` and `keys_in_postgres_not_in_oracle.csv`.

### LOB Load Failure

With `--lob include` or `--lob stream`, LOB read/conversion errors fail the table. Review `failed_row_samples` for table name, chunk key, row number, configured key values, column name, and error message. Do not set `sync.skip_failed_rows: true` unless DBA sign-off accepts partial loads.

## 8. Rollback Strategy

Safe Oracle -> PostgreSQL modes register rollback state automatically:

- `truncate_safe`: restores from `table__backup_<timestamp>`
- `swap_safe`: renames the preserved backup table back into place
- `incremental_safe`: restores the pre-apply target backup

Operationally:

1. capture the failing run ID
2. preserve the run directory
3. disable cron for the affected direction
4. run `ops rollback <run_id> --config config.yaml`
5. verify the restored table and dependency status
6. rerun dry-run or `--simulate` before re-enabling execute jobs

If `backup_before_truncate` was disabled manually, rollback for `truncate_safe` is no longer guaranteed. Keep it enabled in production.

## 9. Circuit Breaker And Alerting

Production execute jobs should treat repeated failure as a stop condition:

- after `sync.max_failures` consecutive failures for the same job key, the job is blocked for `sync.cooldown_minutes`
- a blocked job exits non-zero without touching data
- `job.alert` can send webhook or email payloads for `failure`, `repeated_failure`, and `dependency_error`

Check circuit state:

```bash
ops circuit-breaker list --config config.yaml
```

Reset a circuit only after the failed data path is verified or rolled back:

```bash
ops circuit-breaker reset --table public.sample_customer --config config.yaml
```

Reset every circuit entry only during controlled recovery:

```bash
ops circuit-breaker reset --all --config config.yaml
```

## 10. Post-Change Checklist

After changing config, table scope, or LOB policy:

1. `ops doctor --config config.yaml`
2. `ops audit --config config.yaml`
3. `ops analyze lob --config config.yaml`
4. `ops sync --config config.yaml --simulate`
5. dry-run the affected direction
6. only then return to `--go`
