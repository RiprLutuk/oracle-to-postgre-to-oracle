from __future__ import annotations

from datetime import datetime

from psycopg import sql

from oracle_pg_sync.db.postgres import table_ident
from oracle_pg_sync.utils.naming import pg_old_name, pg_staging_name


def create_staging_like(cur, schema: str, table: str, *, staging_schema: str | None = None) -> tuple[str, str]:
    target_schema = staging_schema or schema
    staging = pg_staging_name(table)
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(target_schema, staging)))
    cur.execute(
        sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
            table_ident(target_schema, staging),
            table_ident(schema, table),
        )
    )
    return target_schema, staging


def atomic_swap(cur, schema: str, table: str) -> str:
    staging = pg_staging_name(table)
    token = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    old_table = pg_old_name(table, token)
    cur.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(table_ident(schema, table)))
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            table_ident(schema, table),
            sql.Identifier(old_table),
        )
    )
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            table_ident(schema, staging),
            sql.Identifier(table),
        )
    )
    return old_table


def drop_table(cur, schema: str, table: str) -> None:
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(schema, table)))
