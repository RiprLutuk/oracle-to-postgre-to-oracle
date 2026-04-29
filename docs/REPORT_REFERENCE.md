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
- `metrics.json`: per-table throughput, bytes, LOB volume, and slow-table summary
- `dependency_pre.csv`: dependency graph before sync/repair
- `dependency_post.csv`: dependency graph after sync/repair
- `dependency_maintenance.csv`: refresh/recompile/validation actions
- `dependency_summary.csv`: rolled-up dependency health
- `rollback_result.csv`: automatic/manual rollback outcome when a rollback was attempted
- `lob_analysis.csv`: LOB analysis command output
- `schema_suggestions.sql`: add/drop column suggestions from audit diff rows
- `logs.txt`: captured run log

## Excel Workbook

Sheets are fixed:

1. `00_Dashboard`
2. `01_Run_Summary`
3. `02_Table_Sync_Status`
4. `03_Rowcount_Compare`
5. `04_Checksum_Result`
6. `05_Column_Diff`
7. `06_Index_Compare`
8. `07_Object_Dependency`
9. `08_LOB_Columns`
10. `09_Failed_Tables`
11. `10_Watermark`
12. `11_Checkpoint`
13. `12_Performance`
14. `13_Errors`
15. `14_Rollback`
16. `15_Timeline`
17. `16_Config`

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
