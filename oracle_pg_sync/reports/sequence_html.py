from __future__ import annotations

from collections import Counter
from html import escape
from pathlib import Path
from typing import Any


SEQUENCE_FIELDS = [
    "compare_status",
    "table_name",
    "postgres_sequence",
    "postgres_column",
    "dependency_kind",
    "oracle_sequence",
    "oracle_last_number",
    "oracle_table_max_value",
    "sequence_buffer",
    "postgres_current_next",
    "table_max_value",
    "postgres_sequence_max_value",
    "postgres_set_to",
    "oracle_pg_delta",
    "status",
    "message",
]


def write_sequence_html_report(path: Path, rows: list[dict[str, Any]], *, title: str = "Oracle PostgreSQL Sequence Compare") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    display_rows = [_display_row(row) for row in rows]
    counts = Counter(row["compare_status"] for row in display_rows)
    manifest_link = '<a href="manifest.json">manifest.json</a>' if (path.parent / "manifest.json").exists() else ""
    csv_link = '<a href="sequence_sync.csv">sequence_sync.csv</a>' if (path.parent / "sequence_sync.csv").exists() else ""
    links = " | ".join(item for item in [manifest_link, csv_link] if item)
    links_html = f"<p>{links}</p>" if links else ""
    needs_set = [row for row in display_rows if row["compare_status"] == "NEEDS_SET"]
    applied = [row for row in display_rows if row["compare_status"] == "SET_APPLIED"]
    pg_ahead = [row for row in display_rows if row["compare_status"] == "PG_AHEAD"]
    aligned = [row for row in display_rows if row["compare_status"] == "ALIGNED"]
    skipped = [row for row in display_rows if row["compare_status"] == "SKIPPED"]

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #f6f7fb;
      --card: #ffffff;
      --line: #d7dce5;
      --ink: #172033;
      --muted: #64748b;
      --head: #e8edf5;
      --ok-bg: #dcfce7;
      --warn-bg: #fef3c7;
      --error-bg: #fee2e2;
      --info-bg: #dbeafe;
      --accent: #2563eb;
    }}
    body {{ font-family: "IBM Plex Sans", "Segoe UI", sans-serif; margin: 24px; color: var(--ink); background: var(--bg); }}
    h1 {{ margin: 0 0 8px; }}
    a {{ color: var(--accent); }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
      margin: 18px 0 22px;
    }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: var(--card);
    }}
    .metric strong {{ display: block; font-size: 26px; margin-top: 4px; }}
    .toolbar {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0 18px; }}
    .toolbar input, .toolbar select {{
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--card);
    }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 26px; font-size: 13px; background: var(--card); }}
    th, td {{ border: 1px solid var(--line); padding: 6px 8px; text-align: left; vertical-align: top; }}
    th {{ background: var(--head); position: sticky; top: 0; z-index: 1; }}
    tr:nth-child(even) {{ background: #f9fafb; }}
    tr.status-aligned {{ background: var(--ok-bg); }}
    tr.status-needs_set {{ background: var(--error-bg); }}
    tr.status-set_applied {{ background: var(--ok-bg); }}
    tr.status-pg_ahead {{ background: var(--info-bg); }}
    tr.status-skipped {{ background: var(--warn-bg); }}
    details {{ margin: 14px 0; }}
    summary {{ cursor: pointer; font-weight: 700; }}
    .muted {{ color: var(--muted); }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  {links_html}
  <p class="muted">Compare Oracle sequence last number, Oracle table max value, optional buffer, PostgreSQL next value, PostgreSQL table max value, and final setval target.</p>
  <div class="toolbar">
    <input id="searchBox" type="search" placeholder="Search table/sequence" oninput="filterRows()">
    <select id="statusFilter" onchange="filterRows()">
      <option value="">All statuses</option>
      <option value="NEEDS_SET">NEEDS_SET</option>
      <option value="SET_APPLIED">SET_APPLIED</option>
      <option value="ALIGNED">ALIGNED</option>
      <option value="PG_AHEAD">PG_AHEAD</option>
      <option value="SKIPPED">SKIPPED</option>
    </select>
  </div>
  <div class="metrics">
    <div class="metric">Total Rows<strong>{len(display_rows)}</strong></div>
    <div class="metric">Needs Set<strong>{counts.get("NEEDS_SET", 0)}</strong></div>
    <div class="metric">Set Applied<strong>{counts.get("SET_APPLIED", 0)}</strong></div>
    <div class="metric">Aligned<strong>{counts.get("ALIGNED", 0)}</strong></div>
    <div class="metric">PG Ahead<strong>{counts.get("PG_AHEAD", 0)}</strong></div>
    <div class="metric">Skipped<strong>{counts.get("SKIPPED", 0)}</strong></div>
  </div>
  {_section("Needs Set", needs_set)}
  {_section("Set Applied", applied)}
  {_section("PostgreSQL Ahead", pg_ahead)}
  {_section("Aligned", aligned)}
  {_section("Skipped", skipped)}
  {_section("All Sequence Compare", display_rows)}
  <script>
    function filterRows() {{
      const query = document.getElementById('searchBox').value.toLowerCase();
      const status = document.getElementById('statusFilter').value.toLowerCase();
      document.querySelectorAll('tbody tr').forEach((row) => {{
        const text = row.innerText.toLowerCase();
        const rowStatus = row.dataset.status || '';
        row.style.display = (!query || text.includes(query)) && (!status || rowStatus === status) ? '' : 'none';
      }});
    }}
  </script>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _display_row(row: dict[str, Any]) -> dict[str, Any]:
    display = {field: row.get(field, "") for field in SEQUENCE_FIELDS}
    oracle_baseline = row.get("oracle_table_max_value") or row.get("oracle_last_number")
    display["oracle_pg_delta"] = _delta(row.get("postgres_current_next"), oracle_baseline)
    display["compare_status"] = _compare_status(row)
    return display


def _compare_status(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "").upper()
    if status == "SKIPPED":
        return "SKIPPED"
    pg_next = _int_or_none(row.get("postgres_current_next"))
    oracle_last = _int_or_none(row.get("oracle_last_number"))
    oracle_table_max = _int_or_none(row.get("oracle_table_max_value"))
    sequence_buffer = _int_or_none(row.get("sequence_buffer")) or 0
    set_to = _int_or_none(row.get("postgres_set_to"))
    if pg_next is None or set_to is None:
        return "SKIPPED"
    if pg_next < set_to:
        if status == "SET":
            return "SET_APPLIED"
        return "NEEDS_SET"
    if sequence_buffer > 0:
        if pg_next > set_to:
            return "PG_AHEAD"
        return "ALIGNED"
    oracle_baseline = oracle_table_max if oracle_table_max is not None else oracle_last
    if oracle_baseline is not None and pg_next > oracle_baseline:
        return "PG_AHEAD"
    return "ALIGNED"


def _delta(left: Any, right: Any) -> str:
    left_value = _int_or_none(left)
    right_value = _int_or_none(right)
    if left_value is None or right_value is None:
        return ""
    return str(left_value - right_value)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _section(title: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    return f"<details open><summary>{escape(title)} ({len(rows)})</summary>{_table(rows)}</details>"


def _table(rows: list[dict[str, Any]]) -> str:
    header = "".join(f"<th>{escape(field)}</th>" for field in SEQUENCE_FIELDS)
    body = []
    for row in rows:
        status = str(row.get("compare_status", "")).lower()
        classes = f"status-{status}"
        body.append(
            f'<tr class="{classes}" data-status="{escape(status)}">'
            + "".join(f"<td>{escape(str(row.get(field, '')))}</td>" for field in SEQUENCE_FIELDS)
            + "</tr>"
        )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"
