# User Guide

`oracle-pg-sync-audit` is a DBA-facing sync and audit tool for Oracle and PostgreSQL. It supports:

- Oracle -> PostgreSQL sync
- PostgreSQL -> Oracle sync
- dry-run by default
- checkpoint and resume
- incremental sync with stored watermarks
- dependency-aware post-sync maintenance
- LOB analysis and LOB policy controls
- centralized Excel and HTML reports
- job wrappers for daily and incremental cron runs

## Install

```bash
cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Primary entrypoints:

```bash
oracle-pg-sync-audit --help
ops --help
```

## Configure

Start from the examples:

```bash
cp .env.example .env
cp config.yaml.example config.yaml
```

Put secrets only in `.env`. Do not commit `.env`, `config.yaml`, or real table lists.

Minimal connection variables:

```dotenv
ORACLE_DSN=oracle-host.example.com:1521/ORCLPDB1
ORACLE_USER=app_reader
ORACLE_PASSWORD=REDACTED
ORACLE_SCHEMA=APP

PG_HOST=postgres-host.example.com
PG_PORT=5432
PG_DATABASE=appdb
PG_USER=sync_user
PG_PASSWORD=REDACTED
PG_SCHEMA=public
```

Declare tables inline or by `tables_file`. Typical production usage is `tables_file: configs/tables.yaml`.

Example table entry:

```yaml
tables:
  - name: public.address
    directions: [oracle-to-postgres, postgres-to-oracle]
    oracle_to_postgres_mode: truncate_safe
    postgres_to_oracle_mode: upsert
    key_columns: [address_id]
    incremental:
      enabled: true
      strategy: updated_at
      column: last_update
      overlap_minutes: 5
    validation:
      checksum:
        enabled: true
```

## Audit And Smart Schema Diff

Run a schema/data audit:

```bash
oracle-pg-sync-audit audit --config config.yaml
```

Important diff behavior:

- column order only -> `INFO`, not a mismatch
- compatible aliases such as `NUMBER(38,0)` vs `numeric(38,0)` -> not a mismatch
- Oracle `DATE` vs PostgreSQL `timestamp` -> `INFO`
- narrower PostgreSQL target types -> `ERROR`
- missing columns -> `ERROR`

Useful audit variants:

```bash
oracle-pg-sync-audit audit --config config.yaml --tables public.address public.housemaster
oracle-pg-sync-audit audit --config config.yaml --all-postgres-tables --fast-count
oracle-pg-sync-audit audit --config config.yaml --workers 4 --suggest-drop
oracle-pg-sync-audit audit-objects --config config.yaml
```

## Sync Oracle -> PostgreSQL

Production-safe modes:

- `truncate_safe`: load to `_stg_<table>_<run_id>`, validate rowcount + checksum, then truncate and refill from staging
- `swap_safe`: load and validate a new table, atomically rename, keep `table__backup_<timestamp>` for rollback
- `incremental_safe`: load changed rows to staging, validate them, back up target, then apply staged upsert

Dry-run is the default:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.address
```

Execute the load:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.address --go
```

Run all configured Oracle -> PostgreSQL tables:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --go
```

Risk simulation:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --simulate
```

Rollback by run ID:

```bash
ops rollback <run_id> --config config.yaml
```

Daily full-refresh defaults:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --profile daily --go
```

## Sync PostgreSQL -> Oracle

Reverse sync supports:

- `truncate`
- `append`
- `delete`
- `upsert`
- checkpoint/resume
- incremental watermark filters
- optional checksum validation
- Oracle `MERGE` for upsert
- LOB policies

Dry-run:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.address --mode upsert
```

Execute:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.address --mode upsert --go
```

Single-table incremental override from CLI:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.address \
  --mode upsert \
  --key-columns address_id \
  --incremental-column last_update \
  --incremental \
  --go
```

## Incremental Sync, Watermarks, Resume

Checkpoint and watermark commands:

```bash
ops status --config config.yaml
ops resume --config config.yaml
ops resume RUN_ID --config config.yaml
ops watermarks --config config.yaml
ops reset-watermark public.address --config config.yaml
oracle-pg-sync-audit sync --config config.yaml --list-runs
```

Profiles:

- `--profile daily` -> defaults to `truncate_safe` full refresh
- `--profile every_5min` -> defaults to `incremental_safe` plus `--incremental`

Notes:

- reverse sync currently checkpoints at full-table phase granularity
- Oracle -> PostgreSQL chunk/checkpoint state is stored in the same SQLite checkpoint database
- watermarks are stored by direction, table, strategy, and column
- safe-mode watermark updates are deferred until the full run and dependency maintenance succeed

## LOB Handling

Supported Oracle-side LOB families:

- `BLOB` -> PostgreSQL `bytea`
- `CLOB` / `NCLOB` -> PostgreSQL `text`
- `LONG` -> PostgreSQL `text`
- `LONG RAW` -> PostgreSQL `bytea`

Default behavior is conservative:

- `lob_strategy.default: error`
- no LOB content is copied unless a table or column strategy allows it

LOB strategies:

- `error`
- `skip`
- `null`
- `stream`
- `include` (normalized internally to `stream`)

Analyze LOB-heavy tables:

```bash
ops analyze lob --config config.yaml
ops analyze lob --config config.yaml --tables public.address
```

LOB analysis reports classification plus recommendation:

- `exclude`
- `partial_columns`
- `stream`

## Dependency Lifecycle

Dependency commands:

```bash
ops dependencies check --config config.yaml
ops dependencies repair --config config.yaml
```

Lifecycle on execute/repair:

1. collect dependency graph
2. refresh dependent PostgreSQL materialized views
3. detect invalid Oracle objects
4. recompile invalid Oracle view/package/function/procedure objects in a loop
5. validate dependent PostgreSQL objects still exist
6. fail the run if critical dependency rows remain and `dependency.fail_on_broken_dependency` is true

If dependency maintenance still fails after the configured repair loop, the sync run is marked failed, rollback is attempted for safe modes, and alerts are emitted when configured.

The dependency commands write run-scoped CSV, Excel, and HTML reports.

## Doctor

Run environment checks:

```bash
ops doctor --config config.yaml
ops doctor --offline --config config.yaml
```

Current checks include:

- config load
- table config presence

## Production Workflow

Recommended production sequence:

1. `ops doctor --config config.yaml`
2. `ops audit --config config.yaml`
3. `ops sync --config config.yaml --direction oracle-to-postgres --simulate`
4. `ops sync --config config.yaml --direction oracle-to-postgres --profile daily --go`
5. `ops report latest --config config.yaml`

For cron:

- `jobs/daily.sh oracle_to_pg` should use the safe daily profile
- `jobs/incremental.sh oracle_to_pg` should use `incremental_safe` on tables with a protected watermark
- keep `ops rollback <run_id>` in the on-call procedure for every execute job
- checkpoint path
- lock file path
- disk space
- Oracle connectivity and dictionary visibility
- PostgreSQL connectivity
- PostgreSQL `pgcrypto` extension visibility
- PostgreSQL schema `USAGE`
- PostgreSQL schema `CREATE`
- dependency health on the first configured tables

## Reports

Latest run report:

```bash
ops report latest --config config.yaml
oracle-pg-sync-audit report --config config.yaml
```

Each run writes to:

```text
reports/run_<timestamp>_<run_id>/
```

The standard workbook is `report.xlsx` and the standard dashboard is `report.html`.

## Cron Jobs

Direction-aware wrappers:

```bash
./jobs/daily.sh oracle_to_pg
./jobs/daily.sh pg_to_oracle
./jobs/incremental.sh oracle_to_pg
./jobs/incremental.sh pg_to_oracle
```

Examples:

```bash
./jobs/daily.sh oracle_to_pg
./jobs/incremental.sh pg_to_oracle --tables public.address --mode upsert --key-columns address_id --incremental-column last_update
```

Job wrapper behavior:

- lock file per profile and direction
- retry loop
- timeout
- optional alert command via `ALERT_COMMAND`
- log rotation by size
- rotated log cleanup

See [`jobs/crontab.example`](../jobs/crontab.example) for cron lines.

## Common CLI Examples

```bash
ops audit --config config.yaml
ops sync --config config.yaml
ops sync --config config.yaml --go
ops resume --config config.yaml
ops status --config config.yaml
ops report latest --config config.yaml
ops analyze lob --config config.yaml
ops dependencies check --config config.yaml
ops dependencies repair --config config.yaml
ops doctor --config config.yaml
```

## Safety Notes

- existing CLI names are preserved
- dry-run remains the default
- reports sanitize config output before writing
- sync and checksum code use streaming/batched reads rather than `fetchall` for large-row paths
