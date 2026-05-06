# User Guide

Untuk panduan Bahasa Indonesia yang lebih ringkas untuk operator, mulai dari
[Panduan Operator Awam](OPERATOR_QUICK_START_ID.md).

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

If your Oracle account requires Thick mode, install Oracle Instant Client in a
project-local folder such as `vendor/oracle/instantclient_23_26` and set
`ORACLE_CLIENT_LIB_DIR` in `.env`. See
[Oracle Client Install](ORACLE_CLIENT_INSTALL.md).

## Environment Configuration

`.env` is loaded automatically for every `ops` and `oracle-pg-sync-audit` command. You do not need to manually `export` variables before running the tool.

Use a custom environment file when switching environments:

```bash
ops --env-file .env.prod doctor --config config.yaml
ops --env-file .env.dev sync --config config.yaml --direction oracle-to-postgres --tables A_HP_BATCH
oracle-pg-sync-audit --env-file .env.prod audit --config config.yaml
```

Already exported variables are not overwritten by `.env`; shell exports take priority.

Required variables before database connections:

- `ORACLE_HOST`
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `PG_HOST`
- `PG_PORT`
- `PG_DATABASE`
- `PG_USER`
- `PG_PASSWORD`

If a config placeholder such as `${PG_HOST}` cannot be resolved, the command fails before any DB connection attempt:

```text
Environment variable PG_HOST is not set. Check .env or export it.
```

Troubleshooting:

```bash
echo $PG_HOST
cat .env
ops doctor --config config.yaml
ops --env-file .env.prod doctor --config config.yaml
```

Keep `.env` lines in `KEY=value` format with no spaces around `=`.

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

Typical production usage is `tables_file: configs/tables.yaml`, with that file
kept list-only:

```yaml
tables:
  - public.sample_customer
  - public.sample_order
```

Keep per-table defaults and overrides in `config.yaml`. If your team prefers a
separate snippet file such as `table_overrides.yaml`, merge it into
`config.yaml` before runtime; the loader does not read that file directly.

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
oracle-pg-sync-audit audit --config config.yaml --tables public.sample_customer public.sample_order
oracle-pg-sync-audit audit --config config.yaml --all-postgres-tables --fast-count
oracle-pg-sync-audit audit --config config.yaml --workers 4 --suggest-drop
oracle-pg-sync-audit audit-objects --config config.yaml
```

When no table-level `where` filter is configured, audit logs omit the `where`
suffix. If `where=...` appears, the table is being audited with that effective
filter.

## Sync Oracle -> PostgreSQL

Production-safe modes:

- `truncate`: directly truncate the live PostgreSQL table, then COPY source rows into it. Fast, direct, and destructive if the load fails.
- `truncate_safe`: load to `_stg_<table>_<run_id>`, validate rowcount + checksum, then truncate and refill from staging
- `swap`: build a replacement table and swap it when allowed by `sync.allow_swap`
- `swap_safe`: load and validate a new table, atomically rename, keep `table__backup_<timestamp>` for rollback
- `incremental_safe`: load changed rows to staging, validate them, back up target, then apply staged upsert

Dry-run is the default:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.sample_customer
```

Execute the load:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.sample_customer --go
```

Skip the load when a full refresh table already has the same rowcount on both sides:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.sample_customer --go --skip-if-rowcount-match
```

This pre-check only applies to Oracle -> PostgreSQL full refresh modes without a
`WHERE` filter or active incremental watermark.

Direct truncate with LOB content:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables A_HP_BATCH --lob include --mode truncate --go
```

`--mode truncate` now stays direct truncate. Use `--mode truncate_safe` when you want staging validation before the live target is truncated.

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
ops sync --config config.yaml --direction postgres-to-oracle --tables public.sample_customer --mode upsert
```

Execute:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.sample_customer --mode upsert --go
```

Single-table incremental override from CLI:

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

## Parallel Sync

Parallel sync uses shared PostgreSQL pooling plus worker-local Oracle connections so workers do not repeatedly reconnect or re-resolve DNS.

CLI flags:

- `--workers N`
- `--parallel-tables`
- `--parallel-chunks`
- `--max-db-connections N`
- `--respect-dependencies`

Example, three tables in parallel:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables A_HP_BATCH A_HP_BATCH_DETAIL ADDRESS \
  --workers 4 \
  --parallel-tables \
  --max-db-connections 5 \
  --mode truncate_safe \
  --go
```

Recommended usage:

- use `--parallel-tables` for independent tables and safe modes such as `truncate_safe`, `swap_safe`, or `incremental_safe`
- use `--parallel-chunks` for a single large table in `append` or `incremental_safe`
- keep `--max-db-connections` close to `--workers` unless you have validated spare DB capacity
- use `--respect-dependencies` when table order matters more than throughput

Safety rules:

- `truncate_safe` and `swap_safe` still validate staging before cutover
- watermark updates are still applied only after the run succeeds
- checkpoint writes are atomic and chunk claiming prevents duplicate work on resume
- when multiple tables are already running in parallel, chunk parallelism is disabled for that run to avoid over-committing DB connections

Config example:

```yaml
sync:
  workers: 1
  parallel_tables: false
  parallel_chunks: false
  max_db_connections: 5
  respect_dependencies: false
```

## Incremental Sync, Watermarks, Resume

Checkpoint and watermark commands:

```bash
ops status --config config.yaml
ops resume --config config.yaml
ops resume RUN_ID --config config.yaml
ops watermarks --config config.yaml
ops reset-watermark public.sample_customer --config config.yaml
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

With `--lob include` or `--lob stream`, Oracle LOB objects are read with `read()`. BLOB-like values are sent to PostgreSQL `bytea` in hex format, and CLOB/NCLOB/LONG text has embedded NUL bytes removed. LOB read errors fail the table instead of silently dropping rows.

Checksum excludes LOB columns by default because Oracle BLOB/CLOB and PostgreSQL bytea/text can have different binary representations even when the application value is correct. Use separate LOB validation by size or hash when you need LOB-specific proof.

Analyze LOB-heavy tables:

```bash
ops analyze lob --config config.yaml
ops analyze lob --config config.yaml --tables public.sample_customer
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

## Rowcount And Key Validation

Every successful Oracle -> PostgreSQL load validates rowcount by default:

- Oracle count uses the resolved `source_schema`, `source_table`, and `effective_where`
- PostgreSQL count uses the resolved `target_schema` and `target_table`
- safe modes validate staging before cutover
- a mismatch fails the table when `validation.rowcount.fail_on_mismatch: true`

Useful commands:

```bash
ops audit --config config.yaml --tables A_HP_BATCH
ops sync --config config.yaml --direction oracle-to-postgres --tables A_HP_BATCH --rowcount-only
ops validate --config config.yaml --tables A_HP_BATCH --missing-keys
ops validate missing-keys --config config.yaml --tables A_HP_BATCH
ops report latest --config config.yaml
```

Missing-key validation uses `key_columns` or `primary_key` from config when present. If not, it tries the table `PRIMARY KEY` first and then a `UNIQUE` constraint from Oracle/PostgreSQL. It writes:

- `keys_in_oracle_not_in_postgres.csv`
- `keys_in_postgres_not_in_oracle.csv`

Missing-key validation uses full sorted streaming comparison. The final status
is based on the complete key stream; `sample_limit` only limits how many detail
rows are written to the CSV files.

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
./jobs/incremental.sh pg_to_oracle --tables public.sample_customer --mode upsert --key-columns customer_id --incremental-column updated_at
```

Per-minute local reverse wrapper example:

```bash
P2O_1MIN_DRY_RUN=1 ./jobs/pg_to_oracle_every_1min.sh
P2O_1MIN_DRY_RUN=0 ./jobs/pg_to_oracle_every_1min.sh
```

`jobs/pg_to_oracle_every_1min.sh` is intentionally ignored by git because the
table/key list is environment-specific. It writes one centralized summary log
to `reports/job_logs/every_1min_pg_to_oracle.log`; raw per-table logs are kept
only for failures unless `P2O_1MIN_KEEP_RAW_LOGS=1` is set. See
[`DBA_DAILY_OPERATIONS.md`](DBA_DAILY_OPERATIONS.md#cron-postgresql---oracle-per-menit)
for a complete crontab example.

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
