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
) -> int:
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        table_ident(schema, table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )
    copied = 0
    with pg_cur.copy(copy_sql) as copy:
        for row in rows:
            copy.write_row([_sanitize_value(value) for value in row])
            copied += 1
    return copied


def _sanitize_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "read"):
        value = value.read()
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value
