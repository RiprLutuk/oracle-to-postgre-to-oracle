# Report Reference

Every run writes to:

```text
reports/run_<timestamp>_<run_id>/
```

The workbook name is `report.xlsx` and the HTML dashboard is `report.html`.

## Standard Files

- `manifest.json`: sanitized run metadata and file inventory
- `inventory_summary.csv`: per-table audit summary
- `column_diff.csv`: smart schema diff rows
- `type_mismatch.csv`: incompatible type-only subset
- `sync_result.csv`: per-table sync results
- `validation_checksum.csv`: checksum output when enabled
- `rowcount_validation.csv`: rowcount-only validation output
- `keys_in_oracle_not_in_postgres.csv`: missing-key samples from Oracle source
- `keys_in_postgres_not_in_oracle.csv`: extra-key samples from PostgreSQL target
- `metrics.json`: per-table throughput, bytes, LOB volume, and slow-table summary
- `dependency_pre.csv`: dependency graph before sync/repair
- `dependency_post.csv`: dependency graph after sync/repair
- `dependency_maintenance.csv`: refresh/recompile/validation actions
- `dependency_summary.csv`: rolled-up dependency health
- `rollback_result.csv`: automatic/manual rollback outcome when a rollback was attempted
- `lob_analysis.csv`: LOB analysis command output
- `schema_suggestions.sql`: add/drop column suggestions from audit diff rows
- `logs.txt`: captured run log

## Cron Job Logs

Production wrapper logs live under:

```text
reports/job_logs/*.log
reports/job_logs/<profile>/*.log
```

These files are central operational logs, not per-run forensic logs. Current
job wrappers intentionally avoid writing fragile `run_.../logs.txt` paths in
the central log because run directories are compacted into:

```text
reports/cron_runs/<profile>/latest/
reports/cron_runs/<profile>/run_history.csv
```

Healthy examples:

```text
profile=<name> phase=<phase> status=COMPLETED exit_code=0 tables=<count> succeeded=<count> failed=0 skipped=0 rows_processed=<rows>
profile=<name> phase=validate status=COMPLETED exit_code=0 rowcount_checked=<count> rowcount_mismatch=0
```

Field meanings:

- `phase`: job phase, such as `incremental`, `truncate`, `sequences`, or
  `validate`.
- `tables` / `total_tables`: number of tables in that phase or full wrapper run.
- `succeeded`, `failed`, `skipped`: table outcome counts.
- `rows_processed`: rows loaded/written for that phase. On incremental jobs,
  `0` usually means no new rows in the current window.
- `sequences_set`: PostgreSQL sequences advanced from Oracle metadata.
- `rowcount_checked`: tables validated by rowcount.
- `rowcount_mismatch`: rowcount validation mismatches. Any non-zero value needs
  investigation.
- `raw_log`: written only on failure and points to the detailed command output.

## Excel Workbook

The workbook always includes the dashboard and run summary. Detail sheets are
written only when they contain data, so a run does not get empty duplicate tabs.

1. `00_Dashboard`
2. `01_Run_Summary`

These detail sheets appear when relevant rows exist:

- `02_Table_Sync_Status`
- `03_Rowcount_Compare`
- `04_Checksum_Result`
- `05_Column_Diff`
- `06_Index_Compare`
- `07_Object_Dependency`
- `08_LOB_Columns`
- `09_Failed_Tables`
- `10_Watermark`
- `11_Checkpoint`
- `12_Performance`
- `13_Errors`
- `14_Rollback`
- `15_Timeline`
- `16_Config`

### `00_Dashboard`

Top-level run metrics:

- total tables
- success count
- failed count
- schema diff `ERROR` count
- schema diff `WARNING` count
- schema diff `INFO` count
- checksum pass/fail
- rows processed
- watermark updates
- resume usage
- LOB-heavy table count
- slow-table count
- validation pass/fail counts in `manifest.json`

`sync_success_but_validation_failed` is always reported as zero in the manifest. A table with failed validation is marked `FAILED` before the run can count it as a sync success.

### `03_Rowcount_Compare`

Important columns:

- `source_schema`
- `source_table`
- `target_schema`
- `target_table`
- `effective_where`
- `oracle_row_count`
- `postgres_row_count`
- `row_count_match`
- `row_count_diff`
- `oracle_count_sql_summary`
- `postgres_count_sql_summary`
- `validation_status`

### `05_Column_Diff`

Columns:

- `table_name`
- `column_name`
- `oracle_type`
- `postgres_type`
- `oracle_ordinal`
- `postgres_ordinal`
- `diff_type`
- `compatibility_status`
- `severity`
- `reason`
- `suggested_action`
- `suggested_pg_type`

## Severity Model

Compatibility statuses:

- `compatible_exact`
- `compatible`
- `compatible_with_warning`
- `incompatible`

Severities:

- `OK`
- `INFO`
- `WARNING`
- `ERROR`

Current interpretation:

- `INFO` = valid difference that should not fail the table
- `WARNING` = technically loadable but review is advised
- `ERROR` = real mismatch or broken dependency

## Mismatch vs Compatible

A table is counted as `MISMATCH` when the audit summary has schema diff `ERROR` rows or a table is missing.

Examples that are not counted as mismatches:

- column ordinal changes only
- Oracle `DATE` vs PostgreSQL `timestamp`
- compatible aliases such as Oracle `VARCHAR2` vs PostgreSQL `varchar`

Examples that are counted as mismatches:

- missing column in PostgreSQL
- extra column in PostgreSQL when compared to Oracle
- narrower PostgreSQL numeric/character target type
- incompatible LOB target type

## HTML Dashboard

The HTML report:

- links to `manifest.json` and `report.xlsx`
- highlights `ERROR` rows
- filters by table status and severity
- hides `INFO` diff rows by default
- shows dependency, checksum, failure timeline, rollback, LOB, and sync problem sections

## Dependency Reporting

Dependency sheets and CSVs use:

- `broken_count`
- `invalid_count`
- `missing_count`
- `failed_count`

`dependency_maintenance.csv` now also carries repair-loop attempts, whether an object was fixed, and whether invalid objects still remained after the final attempt.

When `dependency.fail_on_broken_dependency` is enabled, any critical dependency rows can make an execute or repair run exit non-zero.

## LOB Reporting

LOB analysis and sync rows show:

- LOB classification: `normal`, `LOB-heavy`, `binary-heavy`
- source/target type
- strategy
- validation mode
- recommendation: `exclude`, `partial_columns`, `stream`

Sync rows also include:

- `lob_copy_status`
- `lob_columns_included`
- `lob_columns_skipped`
- `lob_columns_nullified`
- `lob_columns_excluded_from_checksum`

## Watermarks And Checkpoints

`10_Watermark` and `11_Checkpoint` are pulled from the SQLite checkpoint store. They are the operational view for resume and incremental state.

## Performance And Failure Metrics

`metrics.json` and `12_Performance` expose:

- `rows_per_second`
- `bytes_processed`
- `bytes_per_second`
- `lob_bytes_processed`
- `elapsed_seconds`
- `error_rate`
- rollback availability

`manifest.json` now also includes:

- `metrics_summary`
- `rollback_summary`
- `failure_timeline`
- `validation_summary`
- `result_rows` with per-table rowcount, copy metrics, source/target mapping, validation status, failed row samples, and missing-key report file names
