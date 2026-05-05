# Troubleshooting

## `ops doctor` reports `oracle_connection,ERROR`

Check:

- DSN or host/service configuration
- Oracle listener reachability
- Oracle client library path when thick mode is required
- account visibility into the configured schema

## `postgres_pgcrypto,WARNING`

Checksum SQL for PostgreSQL expects `pgcrypto` to be installed when hashing is used.

Install:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

## `ops validate` looks stuck on a large table

Rowcount validation uses exact `SELECT COUNT(1)` by default, so large Oracle/PostgreSQL tables can take a while before the next log line appears.

Use:

```bash
ops validate --config config.yaml --tables public.big_table --fast-count
```

Notes:

- `--fast-count` uses table statistics only when no `WHERE` filter is active
- when a table config has `where: ...`, validation falls back to exact count for that side
- logs now state whether a side is using statistic count or exact `SELECT COUNT(1)`

## Audit shows `MISMATCH`

Open `column_diff.csv` or the `05_Column_Diff` sheet and look at:

- `severity`
- `reason`
- `suggested_action`

Remember:

- `INFO` rows are not counted as mismatches
- `ERROR` rows are

## Sync exits because of dependency health

Run:

```bash
ops dependencies check --config config.yaml
ops dependencies repair --config config.yaml
```

If repair still leaves critical rows, fix the invalid object directly in Oracle or PostgreSQL and rerun.

## Reverse sync skips a table

Common causes:

- schema diff has fatal `ERROR` rows
- no key columns for `upsert`
- all mapped columns were removed by LOB policy
- a successful checkpoint chunk already exists and `--resume` was used

## LOB execute fails

Default LOB behavior is `error`. Configure a table or column strategy:

- `skip`
- `null`
- `stream`

Use:

```bash
ops analyze lob --config config.yaml
```

## Job wrapper exits non-zero

Check:

- `reports/job_logs/*.log`
- lock file collisions under `reports/locks/`
- timeout too low
- alert hook side effects

Run the same sync command manually with `ops sync ...` to reproduce outside cron.

## Resume does not continue where expected

Inspect:

```bash
oracle-pg-sync-audit sync --config config.yaml --list-runs
ops status --config config.yaml
```

If a watermark is wrong, reset it with `ops reset-watermark`.

## HTML report looks empty

The HTML dashboard is run-scoped. Make sure you are opening the report in the latest `reports/run_<timestamp>_<run_id>/` directory rather than the root `reports/` folder.
