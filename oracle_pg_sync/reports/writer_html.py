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
COLUMN_DIFF_FIELDS = [
    "table_name",
    "column_name",
    "oracle_type",
    "postgres_type",
    "oracle_ordinal",
    "postgres_ordinal",
    "diff_type",
    "compatibility_status",
    "severity",
    "reason",
    "suggested_action",
]
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
    "recommendation",
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
ROLLBACK_FIELDS = ["run_id", "table_name", "action_type", "backup_table", "status", "message"]
TIMELINE_FIELDS = ["event_time", "table_name", "phase", "status", "message"]


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
    rollback_rows: list[dict] | None = None,
    timeline_rows: list[dict] | None = None,
    include_empty_sections: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sync_rows = sync_rows or []
    table_rows = sync_rows or inventory_rows
    status_counts = Counter(row.get("status", "UNKNOWN") for row in table_rows)
    top_rows = sorted(
        inventory_rows or sync_rows,
        key=lambda row: int(row.get("oracle_row_count") or 0),
        reverse=True,
    )[:10]
    rowcount_mismatch = [row for row in (inventory_rows or sync_rows) if not row.get("row_count_match")]
    failed_sync = [row for row in sync_rows if row.get("status") in {"FAILED", "WARNING", "SKIPPED"}]
    checksum_rows = checksum_rows or [row for row in sync_rows if row.get("checksum_status")]
    dependency_rows = dependency_rows or []
    dependency_summary_rows = dependency_summary_rows or []
    maintenance_rows = maintenance_rows or []
    rollback_rows = rollback_rows or []
    timeline_rows = timeline_rows or []
    lob_rows = lob_rows or [row for row in sync_rows if row.get("lob_columns_detected") or row.get("lob_type")]
    column_diff_rows = _combine_column_diff_rows(column_diff_rows)
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
    :root {{
      --bg: linear-gradient(180deg, #f5efe1 0%, #fffdf7 42%, #f8f4ec 100%);
      --card: rgba(255, 251, 242, 0.95);
      --line: #d8cdb8;
      --ink: #1f2937;
      --muted: #6b7280;
      --error: #b91c1c;
      --error-bg: #fee2e2;
      --warn-bg: #fef3c7;
      --ok-bg: #dcfce7;
      --info-bg: #dbeafe;
      --head: #efe5d0;
      --accent: #8a5a1f;
    }}
    body {{ font-family: "IBM Plex Sans", "Segoe UI", sans-serif; margin: 24px; color: var(--ink); background: var(--bg); }}
    h1, h2 {{ margin-bottom: 8px; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin: 16px 0 24px;
    }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 12px;
      background: var(--card);
      box-shadow: 0 10px 30px rgba(138, 90, 31, 0.08);
    }}
    .metric strong {{ display: block; font-size: 24px; margin-top: 4px; }}
    .toolbar {{ display: flex; gap: 12px; margin: 12px 0 20px; flex-wrap: wrap; }}
    .toolbar input, .toolbar select {{
      padding: 8px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.9);
    }}
    .toolbar label {{ display: inline-flex; align-items: center; gap: 6px; color: var(--muted); }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; font-size: 13px; background: rgba(255,255,255,0.82); }}
    th, td {{
      border: 1px solid var(--line);
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ background: var(--head); }}
    tr:nth-child(even) {{ background: #fafafa; }}
    tr.status-failed, tr.status-mismatch, tr.status-missing, tr.severity-error {{ background: var(--error-bg); }}
    tr.status-warning, tr.status-skipped, tr.severity-warning {{ background: var(--warn-bg); }}
    tr.status-success, tr.status-match, tr.severity-ok {{ background: var(--ok-bg); }}
    tr.severity-info {{ background: var(--info-bg); }}
    tr.heavy {{ outline: 2px solid #f59e0b; }}
    details {{ margin: 14px 0; }}
    summary {{ cursor: pointer; font-weight: 700; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #f3ead7; color: var(--accent); font-size: 12px; }}
  </style>
</head>
<body>
  <h1>Oracle PostgreSQL Sync Audit</h1>
  {links_html}
  <div class="toolbar">
    <input id="searchBox" type="search" placeholder="Search table/object" oninput="filterTables()">
    <select id="statusFilter" onchange="filterTables()">
      <option value="">All table statuses</option>
      <option value="SUCCESS">SUCCESS</option>
      <option value="MATCH">MATCH</option>
      <option value="WARNING">WARNING</option>
      <option value="MISMATCH">MISMATCH</option>
      <option value="FAILED">FAILED</option>
      <option value="MISSING">MISSING</option>
      <option value="LOB-HEAVY">LOB-heavy</option>
    </select>
    <select id="severityFilter" onchange="filterTables()">
      <option value="">All severities</option>
      <option value="ERROR">ERROR</option>
      <option value="WARNING">WARNING</option>
      <option value="INFO">INFO</option>
      <option value="OK">OK</option>
    </select>
    <label><input id="hideInfo" type="checkbox" checked onchange="filterTables()">Hide INFO rows</label>
  </div>
  <div class="metrics">
    <div class="metric">Total Table<strong>{len(table_rows)}</strong></div>
    <div class="metric">MATCH<strong>{status_counts.get("MATCH", 0)}</strong></div>
    <div class="metric">WARNING<strong>{status_counts.get("WARNING", 0)}</strong></div>
    <div class="metric">MISMATCH<strong>{status_counts.get("MISMATCH", 0)}</strong></div>
    <div class="metric">MISSING<strong>{status_counts.get("MISSING", 0)}</strong></div>
    <div class="metric">Schema ERROR<strong>{sum(1 for row in column_diff_rows if str(row.get("severity", "")).upper() == "ERROR")}</strong></div>
    <div class="metric">Schema INFO<strong>{sum(1 for row in column_diff_rows if str(row.get("severity", "")).upper() == "INFO")}</strong></div>
  </div>
  {_section("Top Table Rowcount Terbesar", top_rows, TOP_ROWCOUNT_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Column Diff", column_diff_rows[:100], COLUMN_DIFF_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Rowcount Mismatch", rowcount_mismatch[:100], TOP_ROWCOUNT_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Dependency Summary", dependency_summary_rows, DEPENDENCY_SUMMARY_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Dependency Object Terbanyak", dependency_heavy, DEPENDENCY_HEAVY_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Sync Bermasalah", failed_sync[:100], SYNC_PROBLEM_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Checksum Validation", checksum_rows[:100], CHECKSUM_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Failure Timeline", timeline_rows[:200], TIMELINE_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Rollback", rollback_rows[:100], ROLLBACK_FIELDS, include_when_empty=include_empty_sections)}
  {_section("LOB Summary", lob_rows[:100], LOB_FIELDS, include_when_empty=include_empty_sections)}
  {_section("Object Dependency", dependency_rows[:100], DEPENDENCY_FIELDS, include_when_empty=include_empty_sections)}
  {_section("MV / View Maintenance", maintenance_rows[:100], MAINTENANCE_FIELDS, include_when_empty=include_empty_sections)}
  <script>
    function filterTables() {{
      const query = document.getElementById('searchBox').value.toLowerCase();
      const status = document.getElementById('statusFilter').value.toLowerCase();
      const severity = document.getElementById('severityFilter').value.toLowerCase();
      const hideInfo = document.getElementById('hideInfo').checked;
      document.querySelectorAll('tbody tr').forEach((row) => {{
        const text = row.innerText.toLowerCase();
        const statusMatch = !status || text.includes(status);
        const severityMatch = !severity || text.includes(severity);
        const queryMatch = !query || text.includes(query);
        const infoMatch = !(hideInfo && row.className.includes('severity-info'));
        row.style.display = statusMatch && severityMatch && queryMatch && infoMatch ? '' : 'none';
      }});
    }}
    filterTables();
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


def _section(title: str, rows: list[dict], fields: list[str], *, include_when_empty: bool) -> str:
    if not rows and not include_when_empty:
        return ""
    return f"<details open><summary>{escape(title)}</summary>{_table(rows, fields)}</details>"


def _row_classes(row: dict) -> list[str]:
    statuses = [
        str(row.get(field, "")).strip().lower().replace(" ", "-")
        for field in ("status", "validation_status", "maintenance_status", "compile_status", "severity")
        if row.get(field)
    ]
    classes = [f"status-{status}" for status in statuses]
    severity = str(row.get("severity", "")).strip().lower().replace(" ", "-")
    if severity:
        classes.append(f"severity-{severity}")
    if str(row.get("classification", "")).upper() == "LOB-HEAVY":
        classes.append("heavy")
    if int(row.get("broken_count") or 0) > 0:
        classes.append("status-failed")
    return classes


def _combine_column_diff_rows(rows: list[dict]) -> list[dict]:
    combined: list[dict] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for row in rows:
        marker = tuple(sorted((str(key), str(value)) for key, value in row.items()))
        if marker in seen:
            continue
        seen.add(marker)
        combined.append(row)
    return combined
