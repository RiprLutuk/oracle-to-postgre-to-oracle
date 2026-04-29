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

4. Review:

- `sync_result.csv`
- `dependency_summary.csv`
- `validation_checksum.csv` if enabled
- `metrics.json`
- `manifest.json`

## 4. First PostgreSQL -> Oracle Execute

Prefer `upsert` with keys for reverse sync.

1. Dry-run:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --mode upsert
```

2. Execute:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --mode upsert --go
```

3. Confirm:

- `status` is `SUCCESS` or an expected `WARNING`
- no checksum mismatch
- no critical dependency failures after the run

## 5. Daily Operations

Recommended operator commands:

```bash
ops status --config config.yaml
ops report latest --config config.yaml
ops dependencies check --config config.yaml
```

Use the run folder as the unit of investigation. Do not mix files from different run directories.

## 6. Cron Deployment

Full refresh:

```bash
jobs/daily.sh oracle_to_pg
jobs/daily.sh pg_to_oracle
```

Incremental:

```bash
jobs/incremental.sh oracle_to_pg
jobs/incremental.sh pg_to_oracle --tables public.address --mode upsert --key-columns address_id --incremental-column last_update
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
ops reset-watermark public.address --config config.yaml
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

## 10. Post-Change Checklist

After changing config, table scope, or LOB policy:

1. `ops doctor --config config.yaml`
2. `ops audit --config config.yaml`
3. `ops analyze lob --config config.yaml`
4. `ops sync --config config.yaml --simulate`
5. dry-run the affected direction
6. only then return to `--go`
