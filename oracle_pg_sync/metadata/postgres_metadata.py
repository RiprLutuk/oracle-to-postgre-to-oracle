from __future__ import annotations

from dataclasses import dataclass

from oracle_pg_sync.metadata.type_mapping import ColumnMeta, postgres_column


@dataclass
class PostgresTableMetadata:
    exists: bool
    row_count: int | None
    columns: list[ColumnMeta]
    object_counts: dict[str, int]


def fetch_table_metadata(
    cur,
    *,
    schema: str,
    table: str,
    fast_count: bool,
) -> PostgresTableMetadata:
    from oracle_pg_sync.db import postgres

    exists = postgres.table_exists(cur, schema, table)
    if not exists:
        return PostgresTableMetadata(False, None, [], {})
    row_count = postgres.fast_count_rows(cur, schema, table) if fast_count else postgres.count_rows(cur, schema, table)
    columns = [postgres_column(row) for row in postgres.get_columns(cur, schema, table)]
    return PostgresTableMetadata(
        exists=True,
        row_count=row_count,
        columns=columns,
        object_counts=postgres.object_counts(cur, schema, table),
    )
