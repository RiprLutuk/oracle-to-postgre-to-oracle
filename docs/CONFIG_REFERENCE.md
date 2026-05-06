# Configuration Reference

## Layout

The runtime model has two layers:

- `.env` for secrets and environment-specific connection values
- `config.yaml` for behavior, table scope, validation, LOB policy, and reporting

The loader supports `${ENV_NAME}` and `${ENV_NAME:-default}` placeholders.

## Root Keys

### `env_file`

Optional dotenv file loaded before config expansion.

### `oracle`

```yaml
oracle:
  dsn: ${ORACLE_DSN:-}
  host: ${ORACLE_HOST}
  port: ${ORACLE_PORT:-1521}
  service_name: ${ORACLE_SERVICE_NAME:-}
  sid: ${ORACLE_SID:-}
  user: ${ORACLE_USER}
  password: ${ORACLE_PASSWORD}
  schema: ${ORACLE_SCHEMA:-}
  client_lib_dir: ${ORACLE_CLIENT_LIB_DIR:-}
```

Use `dsn` directly or let the tool build a DSN from host/port plus service or SID.
`ORACLE_DSN` is optional when `ORACLE_HOST` is used.

### `postgres`

```yaml
postgres:
  host: ${PG_HOST}
  port: ${PG_PORT}
  database: ${PG_DATABASE}
  user: ${PG_USER}
  password: ${PG_PASSWORD}
  schema: ${PG_SCHEMA:-public}
```

### `sync`

Important fields:

- `default_direction`: `oracle-to-postgres` or `postgres-to-oracle`
- `default_mode`: `truncate_safe`, `swap_safe`, `incremental_safe`, `append`, `truncate`, `swap`, `upsert`, or `delete`
- `dry_run`: keep `true` in production configs; execution still requires `--go`
- `fast_count`: use metadata/statistics counts during audit
- `exact_count_after_load`: perform post-load exact rowcount verification
- `workers`
- `parallel_tables`
- `parallel_chunks`
- `max_db_connections`
- `respect_dependencies`
- `parallel_workers`: legacy alias for `workers`
- `batch_size`
- `chunk_size`
- `skip_on_structure_mismatch`
- `build_indexes_on_staging`
- `analyze_after_load`
- `truncate_cascade`
- `allow_swap`
- `max_swap_table_bytes`
- `swap_space_multiplier`
- `keep_old_after_swap`
- `copy_null`
- `pg_lock_timeout`
- `pg_statement_timeout`
- `checkpoint_dir`
- `truncate_resume_strategy`
- `staging_schema`
- `backup_before_truncate`
- `backup_retention_count`
- `staging_retention_count`
- `max_failures`
- `cooldown_minutes`
- `skip_failed_rows`: default `false`; keep false so conversion/COPY errors fail the table
- `failed_row_sample_limit`: maximum row error samples stored in reports
- `skip_if_rowcount_match`: default `false`; for Oracle -> PostgreSQL full refresh only, skip load when source/target rowcount already match before copy

Current checkpoint storage is SQLite and lives under `sync.checkpoint_dir`.

Recommended production values:

```yaml
sync:
  default_mode: truncate_safe
  workers: 1
  parallel_tables: false
  parallel_chunks: false
  max_db_connections: 5
  backup_before_truncate: true
  max_failures: 3
  cooldown_minutes: 30
```

### `reports`

```yaml
reports:
  output_dir: reports
```

### `dependency`

```yaml
dependency:
  auto_recompile_oracle: true
  refresh_postgres_mview: true
  max_recompile_attempts: 3
  max_attempts: 3
  fail_on_broken_dependency: true
```

`max_attempts` controls the full repair loop. The run fails if invalid objects still remain after the last attempt.

### `job`

```yaml
job:
  name: oracle_to_pg_daily
  retry: 3
  timeout_seconds: 3600
  alert_command: echo FAILED
  alert:
    type: webhook
    url: https://hooks.example.net/services/...
    on:
      - failure
      - repeated_failure
      - dependency_error
    timeout_seconds: 10
```

Webhook/email alerts use the structured payload:

```json
{
  "run_id": "abc123",
  "direction": "oracle-to-postgres",
  "error": "dependency validation failed",
  "failed_tables": ["public.sample_customer"]
}
```

Email settings:

```yaml
job:
  alert:
    type: email
    email:
      from_address: sync-bot@example.com
      to: [dba@example.com, oncall@example.com]
      smtp_host: smtp.example.com
      smtp_port: 587
      username: smtp-user
      password: ${SMTP_PASSWORD}
      use_tls: true
```

These fields are part of the config model and show up in sanitized run reports. The shell wrappers under `jobs/` currently read their runtime values from environment variables such as `RETRY`, `TIMEOUT_SECONDS`, and `ALERT_COMMAND`.

### `validation.checksum`

```yaml
validation:
  checksum:
    enabled: true
    mode: table
    columns: auto
    exclude_columns: []
    batch_size: 5000
    chunk_key: null
    sample_percent: 1
    exclude_lob_by_default: true
  rowcount:
    enabled: true
    fail_on_mismatch: true
  missing_keys:
    enabled: true
    sample_limit: 1000
```

Behavior:

- checksum reads rows in batches
- default `columns: auto` excludes LOB columns
- output goes to `validation_checksum.csv` and report sheets when enabled
- rowcount validation runs after successful loads and fails the table on mismatch by default
- missing-key validation compares configured keys and writes sample CSV files

### `lob_strategy`

```yaml
lob_strategy:
  default: error
  stream_batch_size: 100
  lob_chunk_size_bytes: 1048576
  bytea_format: hex
  clob_null_byte_policy: remove
  fail_on_lob_read_error: true
  validation:
    default: size
    hash_algorithm: sha256
  warn_on_lob_larger_than_mb: 50
  fail_on_lob_larger_than_mb: null
  columns:
    public.sample_customer.payload:
      strategy: stream
      target_type: bytea
      validation: size_hash
```

Supported strategies:

- `error`
- `skip`
- `null`
- `stream`
- `include` -> normalized to `stream`

LOB target mapping:

- `BLOB`, `LONG RAW` -> `bytea`
- `CLOB`, `NCLOB`, `LONG` -> `text`

LOB copy behavior:

- `stream` and `include` copy actual LOB content
- `skip` removes the LOB column from the load mapping
- `null` inserts NULL for that column
- `error` fails when a LOB column is detected
- BLOB/RAW bytes are COPY-compatible `bytea` hex values
- CLOB/NCLOB/LONG text preserves content and removes NUL bytes by default

### `rename_columns`

Map Oracle column names to PostgreSQL column names for diff and sync alignment.

```yaml
rename_columns:
  public.sample_customer:
    legacy_status: status
```

### `tables` and `tables_file`

Use one or the other.

Recommended external table file:

```yaml
tables:
  - public.sample_customer
  - public.sample_order
```

Keep `configs/tables.yaml` list-only so scope review stays simple. Put per-table
defaults and overrides inline in `config.yaml`. If your team wants a separate
working file such as `table_overrides.yaml`, merge that content into
`config.yaml` before runtime; the loader only reads `config.yaml` plus
`tables_file`.

## Table-Level Keys

Supported table config keys:

- `name`
- `source_schema`
- `source_table`
- `target_schema`
- `target_table`
- `mode`
- `oracle_to_postgres_mode`
- `postgres_to_oracle_mode`
- `directions`
- `key_columns`
- `primary_key`
- `where`
- `incremental`
- `validation`
- `lob_strategy`

Example inline override block in `config.yaml`:

```yaml
tables:
  - name: public.sample_customer
    directions:
      - oracle-to-postgres
      - postgres-to-oracle
    oracle_to_postgres_mode: truncate_safe
    postgres_to_oracle_mode: upsert
    key_columns:
      - customer_id
```

Manual `--tables` resolution is deterministic:

1. exact configured table `name`
2. `target_schema.target_table`
3. `source_schema.source_table`
4. `target_table`
5. `source_table`

If multiple configured tables match at the same priority, the command fails and lists the ambiguous mappings. Logs include the resolved Oracle source, PostgreSQL target, mode, and where filter.

### Incremental Config

```yaml
incremental:
  enabled: true
  strategy: updated_at
  column: updated_at
  initial_value: 2026-01-01T00:00:00
  overlap_minutes: 5
  delete_detection: false
```

Supported strategies:

- `updated_at`
- `numeric_key`

## Safe Mode Semantics

`truncate`

- truncates the live target directly
- copies Oracle rows into the live target
- validates rows read, rows written, and final rowcount
- fails if any row is lost or failed

`truncate_safe`

- loads source rows into `_stg_<table>_<run_id>`
- validates staging rowcount and checksum before touching target
- optionally creates `table__backup_<timestamp>` before truncation

`swap_safe`

- builds and validates a replacement table
- performs atomic rename cutover
- preserves the previous table as `table__backup_<timestamp>`

`swap`

- uses the swap workflow when enabled by `sync.allow_swap`
- logs the effective mode as `swap`

`incremental_safe`

- loads changed rows into staging
- validates the staged delta
- backs up the target before applying staged upsert
- delays watermark updates until the overall run succeeds

## Smart Schema Diff Semantics

There is no separate diff config block today. Behavior is code-defined and report-visible.

Compatibility status:

- `compatible_exact`
- `compatible`
- `compatible_with_warning`
- `incompatible`

Severity:

- `OK`
- `INFO`
- `WARNING`
- `ERROR`

Current high-value rules:

- ordinal-only drift -> `INFO`
- `NUMBER(38,0)` vs `numeric(38,0)` -> compatible
- `VARCHAR2` vs `varchar` -> compatible
- Oracle `DATE` vs PostgreSQL `timestamp` -> `INFO`
- narrower PostgreSQL target type -> `ERROR`
- missing column -> `ERROR`

`INFO` rows do not make the dashboard count a table as mismatched.

## Type Mapping Summary

Oracle to PostgreSQL guidance used in suggestions:

- `VARCHAR2` / `NVARCHAR2` -> `varchar(n)` or `text`
- `CHAR` / `NCHAR` -> `char(n)`
- `NUMBER(p,0)` -> `smallint` / `integer` / `bigint` / `numeric`
- `NUMBER(p,s)` -> `numeric(p,s)`
- `DATE` / `TIMESTAMP` -> `timestamp`
- `INTERVAL ...` -> `interval`
- `RAW` / `BLOB` / `LONG RAW` -> `bytea`
- `CLOB` / `NCLOB` / `LONG` -> `text`
- `JSON` -> `jsonb`
- `XMLTYPE` -> `text`

## Example Production Skeleton

```yaml
env_file: .env

oracle:
  dsn: ${ORACLE_DSN}
  user: ${ORACLE_USER}
  password: ${ORACLE_PASSWORD}
  schema: ${ORACLE_SCHEMA}

postgres:
  host: ${PG_HOST}
  port: ${PG_PORT}
  database: ${PG_DATABASE}
  user: ${PG_USER}
  password: ${PG_PASSWORD}
  schema: ${PG_SCHEMA:-public}

sync:
  default_direction: oracle-to-postgres
  default_mode: truncate
  dry_run: true
  fast_count: true
  checkpoint_dir: reports/checkpoints/checkpoint.sqlite3

dependency:
  auto_recompile_oracle: true
  refresh_postgres_mview: true
  max_recompile_attempts: 3
  fail_on_broken_dependency: true

lob_strategy:
  default: error

tables_file: configs/tables.yaml
```
