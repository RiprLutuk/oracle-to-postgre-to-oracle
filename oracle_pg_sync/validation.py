from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from oracle_pg_sync.metadata.type_mapping import ColumnMeta


UNSUPPORTED_CHECKSUM_TYPES = {
    "BLOB",
    "CLOB",
    "NCLOB",
    "LONG RAW",
    "BYTEA",
    "JSON",
    "JSONB",
}


def checksum_columns(
    columns: list[ColumnMeta],
    *,
    configured: str | list[str] = "auto",
    exclude_columns: list[str] | None = None,
) -> list[str]:
    exclude = {col.lower() for col in (exclude_columns or [])}
    by_name = {col.normalized_name: col for col in columns}
    if configured != "auto":
        return [str(col).lower() for col in configured if str(col).lower() not in exclude]
    result: list[str] = []
    for col in columns:
        if col.normalized_name in exclude:
            continue
        if is_unsupported_checksum_type(col):
            continue
        result.append(col.normalized_name)
    return [col for col in result if col in by_name]


def is_unsupported_checksum_type(column: ColumnMeta) -> bool:
    data_type = (column.data_type or "").upper()
    udt_name = (column.udt_name or "").upper()
    return data_type in UNSUPPORTED_CHECKSUM_TYPES or udt_name in UNSUPPORTED_CHECKSUM_TYPES


def stable_row_hash(rows: Iterable[Iterable[Any]], columns: list[str]) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps([col.lower() for col in columns], separators=(",", ":")).encode("utf-8"))
    for row in rows:
        normalized = [_normalize_value(value) for value in row]
        digest.update(b"\n")
        digest.update(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def _normalize_value(value: Any) -> Any:
    if value is None:
        return {"null": True}
    if hasattr(value, "read"):
        value = value.read()
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, Decimal):
        return {"decimal": format(value.normalize(), "f")}
    if isinstance(value, datetime):
        if value.tzinfo:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return {"datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, float):
        return {"float": repr(value)}
    if isinstance(value, str):
        return {"str": value.replace("\x00", "")}
    return {"value": str(value)}


def checksum_result_row(
    *,
    table_name: str,
    chunk_key: str,
    source_hash: str,
    target_hash: str,
    row_count_source: int,
    row_count_target: int,
) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "chunk_key": chunk_key,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "row_count_source": row_count_source,
        "row_count_target": row_count_target,
        "status": "MATCH" if source_hash == target_hash and row_count_source == row_count_target else "MISMATCH",
    }
