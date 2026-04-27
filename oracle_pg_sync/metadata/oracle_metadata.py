from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from oracle_pg_sync.metadata.type_mapping import ColumnMeta, oracle_column


@dataclass
class OracleTableMetadata:
    exists: bool
    row_count: int | None
    columns: list[ColumnMeta]
    object_counts: dict[str, int]


def fetch_table_metadata(
    cur,
    *,
    owner: str,
    table: str,
    fast_count: bool,
) -> OracleTableMetadata:
    from oracle_pg_sync.db import oracle

    exists = oracle.table_exists(cur, owner, table)
    if not exists:
        return OracleTableMetadata(False, None, [], {})
    row_count = oracle.fast_count_rows(cur, owner, table) if fast_count else oracle.count_rows(cur, owner, table)
    columns = [oracle_column(row) for row in oracle.get_columns(cur, owner, table)]
    return OracleTableMetadata(
        exists=True,
        row_count=row_count,
        columns=columns,
        object_counts=oracle.object_counts(cur, owner, table),
    )
