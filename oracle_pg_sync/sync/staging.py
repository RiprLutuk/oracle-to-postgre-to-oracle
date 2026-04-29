from __future__ import annotations

from datetime import datetime

from psycopg import sql

from oracle_pg_sync.db.postgres import table_ident
from oracle_pg_sync.utils.naming import pg_old_name, pg_staging_name


def create_staging_like(
    cur,
    schema: str,
    table: str,
    *,
    run_id: str,
    staging_schema: str | None = None,
) -> tuple[str, str]:
    target_schema = staging_schema or schema
    staging = pg_staging_name(table, run_id)
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(target_schema, staging)))
    cur.execute(
        sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
            table_ident(target_schema, staging),
            table_ident(schema, table),
        )
    )
    return target_schema, staging


def create_backup_table(cur, schema: str, table: str, *, token: str) -> str:
    backup_table = pg_old_name(table, token, kind="backup")
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(schema, backup_table)))
    cur.execute(
        sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
            table_ident(schema, backup_table),
            table_ident(schema, table),
        )
    )
    cur.execute(
        sql.SQL("INSERT INTO {} SELECT * FROM {}").format(
            table_ident(schema, backup_table),
            table_ident(schema, table),
        )
    )
    return backup_table


def atomic_swap(cur, schema: str, table: str, *, staging_table: str) -> str:
    token = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    old_table = pg_old_name(table, token, kind="backup")
    cur.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(table_ident(schema, table)))
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            table_ident(schema, table),
            sql.Identifier(old_table),
        )
    )
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            table_ident(schema, staging_table),
            sql.Identifier(table),
        )
    )
    return old_table


def restore_backup_table(cur, schema: str, table: str, backup_table: str) -> str:
    failed_name = pg_old_name(table, datetime.utcnow().strftime("%Y%m%d%H%M%S"), kind="failed")
    cur.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(table_ident(schema, table)))
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            table_ident(schema, table),
            sql.Identifier(failed_name),
        )
    )
    cur.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            table_ident(schema, backup_table),
            sql.Identifier(table),
        )
    )
    return failed_name


def drop_table(cur, schema: str, table: str) -> None:
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(schema, table)))
