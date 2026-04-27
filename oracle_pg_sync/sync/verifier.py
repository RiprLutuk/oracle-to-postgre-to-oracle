from __future__ import annotations

from oracle_pg_sync.db import oracle, postgres


def verify_rowcount(
    *,
    oracle_cur,
    pg_cur,
    oracle_owner: str,
    pg_schema: str,
    oracle_table: str,
    pg_table: str,
) -> tuple[int, int, bool]:
    oracle_count = oracle.count_rows(oracle_cur, oracle_owner, oracle_table)
    pg_count = postgres.count_rows(pg_cur, pg_schema, pg_table)
    return oracle_count, pg_count, oracle_count == pg_count
