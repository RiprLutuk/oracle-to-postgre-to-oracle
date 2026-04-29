from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from psycopg import sql

from oracle_pg_sync.db.postgres import table_ident


@dataclass
class CopyMetrics:
    rows_copied: int = 0
    bytes_processed: int = 0
    lob_bytes_processed: int = 0


def copy_rows(
    pg_cur,
    *,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[tuple[Any, ...]],
    lob_chunk_size_bytes: int = 1024 * 1024,
    metrics: CopyMetrics | None = None,
) -> int:
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        table_ident(schema, table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )
    copied = 0
    with pg_cur.copy(copy_sql) as copy:
        for row in rows:
            sanitized = [_sanitize_value(value, lob_chunk_size_bytes=lob_chunk_size_bytes, metrics=metrics) for value in row]
            copy.write_row(sanitized)
            copied += 1
            if metrics:
                metrics.rows_copied += 1
    return copied


def _sanitize_value(
    value: Any,
    *,
    lob_chunk_size_bytes: int = 1024 * 1024,
    metrics: CopyMetrics | None = None,
) -> Any:
    if value is None:
        return None
    is_lob = hasattr(value, "read")
    if hasattr(value, "read"):
        value = _read_lob_stream(value, chunk_size=max(1, int(lob_chunk_size_bytes or 1024 * 1024)))
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if metrics:
        size = _value_size(value)
        metrics.bytes_processed += size
        if is_lob:
            metrics.lob_bytes_processed += size
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value


def _read_lob_stream(value: Any, *, chunk_size: int) -> Any:
    chunks: list[Any] = []
    offset = 1
    while True:
        try:
            chunk = value.read(offset, chunk_size)
        except TypeError:
            chunk = value.read()
        if not chunk:
            break
        chunks.append(chunk)
        if len(chunk) < chunk_size or not _supports_offset_read(value):
            break
        offset += len(chunk)
    if not chunks:
        return b"" if _is_binary_lob_value(value) else ""
    if isinstance(chunks[0], str):
        return "".join(str(chunk) for chunk in chunks).replace("\x00", "")
    return b"".join(bytes(chunk) for chunk in chunks)


def _supports_offset_read(value: Any) -> bool:
    return value.__class__.__module__.startswith("oracledb") or hasattr(value, "size")


def _is_binary_lob_value(value: Any) -> bool:
    type_name = value.__class__.__name__.upper()
    return "BLOB" in type_name or "RAW" in type_name


def _value_size(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(str(value).encode("utf-8"))
