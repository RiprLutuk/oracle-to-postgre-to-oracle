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
) -> None:
    from oracle_pg_sync.reports.writer_csv import write_csv
    from oracle_pg_sync.reports.writer_excel import write_inventory_xlsx
    from oracle_pg_sync.reports.writer_html import write_html_report

    report_dir.mkdir(parents=True, exist_ok=True)
    write_csv(report_dir / "inventory_summary.csv", inventory_rows)
    write_inventory_xlsx(report_dir / "inventory_summary.xlsx", inventory_rows)
    write_csv(report_dir / "column_diff.csv", column_diff_rows)
    write_csv(report_dir / "type_mismatch.csv", type_mismatch_rows)
    write_csv(report_dir / "object_dependency_summary.csv", dependency_rows)
    if sync_rows is not None:
        write_csv(report_dir / "sync_result.csv", sync_rows)
    write_html_report(
        report_dir / "report.html",
        inventory_rows=inventory_rows,
        column_diff_rows=column_diff_rows,
        sync_rows=sync_rows,
    )
