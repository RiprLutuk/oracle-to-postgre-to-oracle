from __future__ import annotations

from pathlib import Path


def write_audit_reports(
    report_dir: Path,
    *,
    inventory_rows: list[dict],
    column_diff_rows: list[dict],
    type_mismatch_rows: list[dict],
    dependency_rows: list[dict],
    sync_rows: list[dict] | None = None,
    sql_suggestions_path: Path | None = None,
    suggest_drop: bool = False,
) -> None:
    from oracle_pg_sync.reports.writer_csv import write_csv
    from oracle_pg_sync.reports.writer_html import write_html_report
    from oracle_pg_sync.reports.writer_sql import write_schema_suggestions
    from oracle_pg_sync.dependency_health import summarize_dependency_rows

    report_dir.mkdir(parents=True, exist_ok=True)
    write_csv(report_dir / "inventory_summary.csv", inventory_rows)
    write_csv(report_dir / "column_diff.csv", column_diff_rows)
    write_csv(report_dir / "type_mismatch.csv", type_mismatch_rows)
    write_csv(report_dir / "object_dependency_summary.csv", dependency_rows)
    write_schema_suggestions(
        sql_suggestions_path or report_dir / "schema_suggestions.sql",
        column_diff_rows,
        suggest_drop=suggest_drop,
    )
    if sync_rows is not None:
        write_csv(report_dir / "sync_result.csv", sync_rows)
    dependency_summary_rows = summarize_dependency_rows(dependency_rows, [])
    write_html_report(
        report_dir / "report.html",
        inventory_rows=inventory_rows,
        column_diff_rows=column_diff_rows,
        sync_rows=sync_rows,
        dependency_rows=dependency_rows,
        dependency_summary_rows=dependency_summary_rows,
    )
