from __future__ import annotations

from collections import Counter
from html import escape
from pathlib import Path

DEPENDENCY_HEAVY_FIELDS = [
    "table_name",
    "view_count_related_oracle",
    "view_count_related_postgres",
    "stored_procedure_count_related_oracle",
    "function_count_related_postgres",
]
TOP_ROWCOUNT_FIELDS = ["table_name", "oracle_row_count", "postgres_row_count", "status"]
COLUMN_DIFF_FIELDS = ["table_name", "diff_type", "column_name", "oracle_type", "postgres_type"]
SYNC_PROBLEM_FIELDS = ["table_name", "mode", "status", "rows_loaded", "message"]
CHECKSUM_FIELDS = [
    "table_name",
    "chunk_key",
    "status",
    "row_count_source",
    "row_count_target",
    "source_hash",
    "target_hash",
]
DEPENDENCY_SUMMARY_FIELDS = [
    "phase",
    "source_db",
    "table_name",
    "object_count",
    "broken_count",
    "invalid_count",
    "missing_count",
    "failed_count",
]
LOB_FIELDS = [
    "source_db",
    "table_name",
    "classification",
    "column_name",
    "lob_columns_detected",
    "lob_columns_synced",
    "lob_type",
    "target_type",
    "lob_target_type",
    "strategy",
    "lob_strategy_applied",
    "validation_mode",
    "lob_validation_mode",
    "warning",
    "suggestion",
]
DEPENDENCY_FIELDS = [
    "phase",
    "source_db",
    "table_name",
    "object_schema",
    "object_type",
    "object_name",
    "dependency_kind",
    "details",
]
MAINTENANCE_FIELDS = [
    "source_db",
    "object_schema",
    "object_type",
    "object_name",
    "maintenance_status",
    "validation_status",
    "compile_status",
    "error_message",
]


def write_html_report(
    path: Path,
    *,
    inventory_rows: list[dict],
    column_diff_rows: list[dict],
    sync_rows: list[dict] | None = None,
    checksum_rows: list[dict] | None = None,
    lob_rows: list[dict] | None = None,
    dependency_rows: list[dict] | None = None,
    dependency_summary_rows: list[dict] | None = None,
    maintenance_rows: list[dict] | None = None,
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
    checksum_rows = checksum_rows or [row for row in sync_rows if row.get("checksum_status")]
    dependency_rows = dependency_rows or []
    dependency_summary_rows = dependency_summary_rows or []
    maintenance_rows = maintenance_rows or []
    lob_rows = lob_rows or [row for row in sync_rows if row.get("lob_columns_detected") or row.get("lob_type")]
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
        f'<a href="manifest.json">manifest.json</a>'
        if (path.parent / "manifest.json").exists() or path.parent.name.startswith("run_")
        else (
            f'<a href="{escape(str(manifests[0].relative_to(path.parent)))}">manifest.json</a>'
            if manifests
            else ""
        )
    )
    workbook_link = (
        '<a href="report.xlsx">report.xlsx</a>'
        if (path.parent / "report.xlsx").exists()
        else ""
    )
    links = " | ".join(item for item in [manifest_link, workbook_link] if item)
    links_html = f"<p>{links}</p>" if links else ""

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Oracle PostgreSQL Sync Audit</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2 {{ margin-bottom: 8px; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin: 16px 0 24px;
    }}
    .metric {{
      border: 1px solid #d1d5db;
      border-radius: 6px;
      padding: 12px;
      background: #f9fafb;
    }}
    .metric strong {{ display: block; font-size: 24px; margin-top: 4px; }}
    .toolbar {{ display: flex; gap: 12px; margin: 12px 0 20px; }}
    .toolbar input, .toolbar select {{ padding: 8px; border: 1px solid #9ca3af; border-radius: 4px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; font-size: 13px; }}
    th, td {{
      border: 1px solid #d1d5db;
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ background: #eef2f7; }}
    tr:nth-child(even) {{ background: #fafafa; }}
    tr.status-failed, tr.status-mismatch, tr.status-missing {{ background: #fee2e2; }}
    tr.status-warning, tr.status-skipped {{ background: #fef3c7; }}
    tr.status-success, tr.status-match {{ background: #dcfce7; }}
    tr.heavy {{ outline: 2px solid #f59e0b; }}
    details {{ margin: 14px 0; }}
    summary {{ cursor: pointer; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>Oracle PostgreSQL Sync Audit</h1>
  {links_html}
  <div class="toolbar">
    <input id="searchBox" type="search" placeholder="Search table/object" oninput="filterTables()">
    <select id="statusFilter" onchange="filterTables()">
      <option value="">All statuses</option>
      <option value="SUCCESS">SUCCESS</option>
      <option value="MATCH">MATCH</option>
      <option value="WARNING">WARNING</option>
      <option value="MISMATCH">MISMATCH</option>
      <option value="FAILED">FAILED</option>
      <option value="MISSING">MISSING</option>
      <option value="LOB-HEAVY">LOB-heavy</option>
    </select>
  </div>
  <div class="metrics">
    <div class="metric">Total Table<strong>{len(inventory_rows)}</strong></div>
    <div class="metric">MATCH<strong>{status_counts.get("MATCH", 0)}</strong></div>
    <div class="metric">WARNING<strong>{status_counts.get("WARNING", 0)}</strong></div>
    <div class="metric">MISMATCH<strong>{status_counts.get("MISMATCH", 0)}</strong></div>
    <div class="metric">MISSING<strong>{status_counts.get("MISSING", 0)}</strong></div>
  </div>
  {_section("Top Table Rowcount Terbesar", top_rows, TOP_ROWCOUNT_FIELDS)}
  {_section("Column Mismatch", column_diff_rows[:100], COLUMN_DIFF_FIELDS)}
  {_section("Rowcount Mismatch", rowcount_mismatch[:100], TOP_ROWCOUNT_FIELDS)}
  {_section("Dependency Summary", dependency_summary_rows, DEPENDENCY_SUMMARY_FIELDS)}
  {_section("Dependency Object Terbanyak", dependency_heavy, DEPENDENCY_HEAVY_FIELDS)}
  {_section("Sync Bermasalah", failed_sync[:100], SYNC_PROBLEM_FIELDS)}
  {_section("Checksum Validation", checksum_rows[:100], CHECKSUM_FIELDS)}
  {_section("LOB Summary", lob_rows[:100], LOB_FIELDS)}
  {_section("Object Dependency", dependency_rows[:100], DEPENDENCY_FIELDS)}
  {_section("MV / View Maintenance", maintenance_rows[:100], MAINTENANCE_FIELDS)}
  <script>
    function filterTables() {{
      const query = document.getElementById('searchBox').value.toLowerCase();
      const status = document.getElementById('statusFilter').value.toLowerCase();
      document.querySelectorAll('tbody tr').forEach((row) => {{
        const text = row.innerText.toLowerCase();
        const statusMatch = !status || text.includes(status);
        const queryMatch = !query || text.includes(query);
        row.style.display = statusMatch && queryMatch ? '' : 'none';
      }});
    }}
  </script>
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
        classes = " ".join(_row_classes(row))
        body.append(
            f'<tr class="{classes}">'
            + "".join(f"<td>{escape(str(row.get(field, '')))}</td>" for field in fields)
            + "</tr>"
        )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _section(title: str, rows: list[dict], fields: list[str]) -> str:
    return f"<details open><summary>{escape(title)}</summary>{_table(rows, fields)}</details>"


def _row_classes(row: dict) -> list[str]:
    statuses = [
        str(row.get(field, "")).strip().lower().replace(" ", "-")
        for field in ("status", "validation_status", "maintenance_status", "compile_status")
        if row.get(field)
    ]
    classes = [f"status-{status}" for status in statuses]
    if str(row.get("classification", "")).upper() == "LOB-HEAVY":
        classes.append("heavy")
    if int(row.get("broken_count") or 0) > 0:
        classes.append("status-failed")
    return classes
