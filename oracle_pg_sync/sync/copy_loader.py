from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from psycopg import sql

from oracle_pg_sync.db.postgres import table_ident


def copy_rows(
    pg_cur,
    *,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[tuple[Any, ...]],
    lob_chunk_size_bytes: int = 1024 * 1024,
) -> int:
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        table_ident(schema, table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )
    copied = 0
    with pg_cur.copy(copy_sql) as copy:
        for row in rows:
            copy.write_row([_sanitize_value(value, lob_chunk_size_bytes=lob_chunk_size_bytes) for value in row])
            copied += 1
    return copied


def _sanitize_value(value: Any, *, lob_chunk_size_bytes: int = 1024 * 1024) -> Any:
    if value is None:
        return None
    if hasattr(value, "read"):
        value = _read_lob_stream(value, chunk_size=max(1, int(lob_chunk_size_bytes or 1024 * 1024)))
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
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
