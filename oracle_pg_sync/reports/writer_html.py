from __future__ import annotations

from collections import Counter
from html import escape
from pathlib import Path


def write_html_report(
    path: Path,
    *,
    inventory_rows: list[dict],
    column_diff_rows: list[dict],
    sync_rows: list[dict] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sync_rows = sync_rows or []
    status_counts = Counter(row.get("status", "UNKNOWN") for row in inventory_rows)
    top_rows = sorted(
        inventory_rows,
        key=lambda row: int(row.get("oracle_row_count") or 0),
        reverse=True,
    )[:10]
    rowcount_mismatch = [row for row in inventory_rows if not row.get("row_count_match")]
    failed_sync = [row for row in sync_rows if row.get("status") in {"FAILED", "WARNING", "SKIPPED"}]
    checksum_rows = [row for row in sync_rows if row.get("checksum_status")]
    dependency_heavy = sorted(
        inventory_rows,
        key=lambda row: int(row.get("view_count_related_oracle") or 0)
        + int(row.get("view_count_related_postgres") or 0)
        + int(row.get("stored_procedure_count_related_oracle") or 0)
        + int(row.get("function_count_related_postgres") or 0),
        reverse=True,
    )[:10]
    manifests = sorted(path.parent.glob("run_*/manifest.json"), reverse=True)
    manifest_link = (
        f'<p>Latest manifest: <a href="{escape(str(manifests[0].relative_to(path.parent)))}">manifest.json</a></p>'
        if manifests
        else ""
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Oracle PostgreSQL Sync Audit</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2 {{ margin-bottom: 8px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin: 16px 0 24px; }}
    .metric {{ border: 1px solid #d1d5db; border-radius: 6px; padding: 12px; background: #f9fafb; }}
    .metric strong {{ display: block; font-size: 24px; margin-top: 4px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; font-size: 13px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f7; }}
    tr:nth-child(even) {{ background: #fafafa; }}
  </style>
</head>
<body>
  <h1>Oracle PostgreSQL Sync Audit</h1>
  {manifest_link}
  <div class="metrics">
    <div class="metric">Total Table<strong>{len(inventory_rows)}</strong></div>
    <div class="metric">MATCH<strong>{status_counts.get("MATCH", 0)}</strong></div>
    <div class="metric">WARNING<strong>{status_counts.get("WARNING", 0)}</strong></div>
    <div class="metric">MISMATCH<strong>{status_counts.get("MISMATCH", 0)}</strong></div>
    <div class="metric">MISSING<strong>{status_counts.get("MISSING", 0)}</strong></div>
  </div>
  <h2>Top Table Rowcount Terbesar</h2>
  {_table(top_rows, ["table_name", "oracle_row_count", "postgres_row_count", "status"])}
  <h2>Column Mismatch</h2>
  {_table(column_diff_rows[:100], ["table_name", "diff_type", "column_name", "oracle_type", "postgres_type"])}
  <h2>Rowcount Mismatch</h2>
  {_table(rowcount_mismatch[:100], ["table_name", "oracle_row_count", "postgres_row_count", "status"])}
  <h2>Dependency Object Terbanyak</h2>
  {_table(dependency_heavy, ["table_name", "view_count_related_oracle", "view_count_related_postgres", "stored_procedure_count_related_oracle", "function_count_related_postgres"])}
  <h2>Sync Bermasalah</h2>
  {_table(failed_sync[:100], ["table_name", "mode", "status", "rows_loaded", "message"])}
  <h2>Checksum Validation</h2>
  {_table(checksum_rows[:100], ["table_name", "checksum_status", "checksum_source_rows", "checksum_target_rows", "checksum_source_hash", "checksum_target_hash"])}
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _table(rows: list[dict], fields: list[str]) -> str:
    if not rows:
        return "<p>No data.</p>"
    header = "".join(f"<th>{escape(field)}</th>" for field in fields)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{escape(str(row.get(field, '')))}</td>" for field in fields) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"
