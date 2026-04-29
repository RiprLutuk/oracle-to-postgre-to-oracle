from __future__ import annotations

from collections import defaultdict
from typing import Any

BROKEN_TOKENS = {
    "BROKEN",
    "DISABLED",
    "FAILED",
    "INVALID",
    "MISSING",
    "UNUSABLE",
}


def is_broken_dependency(row: dict[str, Any]) -> bool:
    if any(int(row.get(field) or 0) > 0 for field in ("broken_count", "invalid_count", "missing_count", "failed_count")):
        return True
    status_values = [
        row.get("status"),
        row.get("validation_status"),
        row.get("maintenance_status"),
        row.get("compile_status"),
        row.get("dependency_status"),
    ]
    details = str(row.get("details") or "")
    if any(_has_broken_token(value) for value in status_values):
        return True
    return any(f"status={token}" in details.upper() for token in BROKEN_TOKENS)


def critical_dependency_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if is_broken_dependency(row)]


def summarize_dependency_rows(
    dependency_rows: list[dict[str, Any]],
    maintenance_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(_summary_row)
    for row in dependency_rows:
        key = (
            str(row.get("phase") or "dependency"),
            str(row.get("source_db") or ""),
            str(row.get("table_name") or ""),
        )
        summary = grouped[key]
        summary["phase"], summary["source_db"], summary["table_name"] = key
        summary["object_count"] += 1
        if is_broken_dependency(row):
            summary["broken_count"] += 1
        if _has_broken_token(row.get("status")):
            summary["invalid_count"] += 1
    for row in maintenance_rows or []:
        key = ("maintenance", str(row.get("source_db") or ""), str(row.get("table_name") or ""))
        summary = grouped[key]
        summary["phase"], summary["source_db"], summary["table_name"] = key
        summary["object_count"] += 1
        if is_broken_dependency(row):
            summary["broken_count"] += 1
        if _has_broken_token(row.get("validation_status"), token="MISSING"):
            summary["missing_count"] += 1
        if _has_broken_token(row.get("maintenance_status"), token="FAILED") or _has_broken_token(
            row.get("compile_status"),
            token="FAILED",
        ):
            summary["failed_count"] += 1
    return sorted(
        grouped.values(),
        key=lambda item: (
            str(item.get("phase")),
            str(item.get("source_db")),
            str(item.get("table_name")),
        ),
    )


def _summary_row() -> dict[str, Any]:
    return {
        "phase": "",
        "source_db": "",
        "table_name": "",
        "object_count": 0,
        "broken_count": 0,
        "invalid_count": 0,
        "missing_count": 0,
        "failed_count": 0,
    }


def _has_broken_token(value: Any, *, token: str | None = None) -> bool:
    text = str(value or "").upper()
    if not text:
        return False
    if token:
        return token in text
    return any(item in text for item in BROKEN_TOKENS)
