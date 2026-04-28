from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl.styles import PatternFill


def write_inventory_xlsx(path: Path, inventory_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(inventory_rows).to_excel(path, index=False, sheet_name="inventory")


def write_rows_xlsx(path: Path, rows: list[dict], *, sheet_name: str = "rows") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_excel(path, index=False, sheet_name=sheet_name[:31])


def write_central_report_xlsx(
    path: Path,
    *,
    sync_rows: list[dict] | None = None,
    checksum_rows: list[dict] | None = None,
    config_sanitized: dict[str, Any] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sync_rows = sync_rows or []
    checksum_rows = checksum_rows or []
    sheets = {
        "00_Dashboard": [_dashboard_row(sync_rows, checksum_rows)],
        "01_Run_Summary": sync_rows,
        "02_Table_Sync_Status": sync_rows,
        "03_Rowcount_Compare": sync_rows,
        "04_Checksum_Result": checksum_rows,
        "05_Column_Structure_Diff": [],
        "06_Index_Compare": [],
        "07_View_SP_Sequence": [],
        "08_LOB_Columns": [row for row in sync_rows if row.get("lob_columns_detected")],
        "09_Failed_Tables": [row for row in sync_rows if row.get("status") in {"FAILED", "WARNING", "SKIPPED"}],
        "10_Watermark": [],
        "11_Checkpoint_Resume": [],
        "12_Performance": sync_rows,
        "13_Errors_Log": [{"table_name": row.get("table_name"), "message": row.get("message")} for row in sync_rows if row.get("message")],
        "14_Config_Sanitized": _flatten_config(config_sanitized or {}),
    }
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=name[:31])
        workbook = writer.book
        for worksheet in workbook.worksheets:
            _format_sheet(worksheet)


def _dashboard_row(sync_rows: list[dict], checksum_rows: list[dict]) -> dict[str, Any]:
    return {
        "total_tables": len(sync_rows),
        "success": sum(1 for row in sync_rows if row.get("status") == "SUCCESS"),
        "failed": sum(1 for row in sync_rows if row.get("status") == "FAILED"),
        "checksum_pass": sum(1 for row in checksum_rows if row.get("status") == "MATCH"),
        "checksum_fail": sum(1 for row in checksum_rows if row.get("status") == "MISMATCH"),
        "rows_processed": sum(int(row.get("rows_loaded") or 0) for row in sync_rows),
        "resume_usage": sum(1 for row in sync_rows if row.get("run_id")),
    }


def _format_sheet(worksheet) -> None:
    worksheet.freeze_panes = "A2"
    if worksheet.max_row >= 1 and worksheet.max_column >= 1:
        worksheet.auto_filter.ref = worksheet.dimensions
    fills = {
        "SUCCESS": PatternFill("solid", fgColor="C6EFCE"),
        "MATCH": PatternFill("solid", fgColor="C6EFCE"),
        "FAILED": PatternFill("solid", fgColor="FFC7CE"),
        "MISMATCH": PatternFill("solid", fgColor="FFC7CE"),
        "WARNING": PatternFill("solid", fgColor="FFEB9C"),
        "SKIPPED": PatternFill("solid", fgColor="FFEB9C"),
    }
    for row in worksheet.iter_rows(min_row=2):
        for cell in row:
            fill = fills.get(str(cell.value or "").upper())
            if fill:
                cell.fill = fill
    for column in worksheet.columns:
        letter = column[0].column_letter
        width = min(60, max(12, max(len(str(cell.value or "")) for cell in column) + 2))
        worksheet.column_dimensions[letter].width = width


def _flatten_config(value: dict[str, Any], prefix: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            rows.extend(_flatten_config(item, path))
        else:
            rows.append({"key": path, "value": item})
    return rows
