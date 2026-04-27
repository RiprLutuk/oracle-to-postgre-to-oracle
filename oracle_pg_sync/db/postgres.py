from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql

from oracle_pg_sync.config import PostgresConfig


def connect(config: PostgresConfig, *, autocommit: bool = False):
    return psycopg.connect(**config.conninfo(), autocommit=autocommit)


def table_ident(schema: str, table: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind IN ('r', 'p', 'v', 'm')
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def count_rows(cur, schema: str, table: str) -> int:
    cur.execute(sql.SQL("SELECT COUNT(1) FROM {}").format(table_ident(schema, table)))
    return int(cur.fetchone()[0])


def fast_count_rows(cur, schema: str, table: str) -> int | None:
    cur.execute(
        """
        SELECT c.reltuples::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_columns(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT column_name, ordinal_position, data_type, udt_name,
               character_maximum_length, numeric_precision, numeric_scale,
               is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [
        {
            "name": row[0],
            "ordinal": row[1],
            "data_type": row[2],
            "udt_name": row[3],
            "char_length": row[4],
            "numeric_precision": row[5],
            "numeric_scale": row[6],
            "nullable": row[7] == "YES",
            "default": row[8],
        }
        for row in cur.fetchall()
    ]


def object_counts(cur, schema: str, table: str) -> dict[str, int]:
    queries = {
        "index_count_postgres": (
            """
            SELECT COUNT(1)
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
            """,
            (schema, table),
        ),
        "trigger_count_postgres": (
            """
            SELECT COUNT(1)
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
            """,
            (schema, table),
        ),
        "constraint_count_postgres": (
            """
            SELECT COUNT(1)
            FROM pg_constraint co
            JOIN pg_class c ON c.oid = co.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (schema, table),
        ),
        "sequence_count_postgres": (
            """
            SELECT COUNT(1)
            FROM pg_sequences
            WHERE schemaname = %s AND sequencename ILIKE '%%' || %s || '%%'
            """,
            (schema, table),
        ),
        "view_count_related_postgres": (
            """
            SELECT COUNT(DISTINCT vc.oid)
            FROM pg_depend d
            JOIN pg_rewrite r ON r.oid = d.objid
            JOIN pg_class vc ON vc.oid = r.ev_class
            WHERE d.refobjid = (
                SELECT c.oid
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
            )
            AND vc.relkind IN ('v', 'm')
            """,
            (schema, table),
        ),
        "function_count_related_postgres": (
            """
            SELECT COUNT(DISTINCT p.oid)
            FROM pg_depend d
            JOIN pg_proc p ON p.oid = d.objid
            WHERE d.refobjid = (
                SELECT c.oid
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
            )
            """,
            (schema, table),
        ),
    }
    counts: dict[str, int] = {}
    for key, (query, params) in queries.items():
        cur.execute(query, params)
        counts[key] = int(cur.fetchone()[0] or 0)
    return counts


def dependency_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT n2.nspname, c2.relname, c2.relkind::text, n.nspname, c.relname, c.relkind::text
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class c2 ON c2.oid = r.ev_class
        JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
        JOIN pg_class c ON c.oid = d.refobjid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        ORDER BY n2.nspname, c2.relname
        """,
        (schema, table),
    )
    return [
        {
            "source_db": "postgres",
            "table_name": f"{schema}.{table}",
            "object_schema": row[0],
            "object_name": row[1],
            "object_type": row[2],
            "referenced_schema": row[3],
            "referenced_name": row[4],
            "referenced_type": row[5],
        }
        for row in cur.fetchall()
    ]


def truncate_table(cur, schema: str, table: str, *, cascade: bool = False) -> None:
    stmt = sql.SQL("TRUNCATE TABLE {}{}").format(
        table_ident(schema, table),
        sql.SQL(" CASCADE") if cascade else sql.SQL(""),
    )
    cur.execute(stmt)


def analyze_table(cur, schema: str, table: str) -> None:
    cur.execute(sql.SQL("ANALYZE {}").format(table_ident(schema, table)))


def set_local_timeouts(cur, *, lock_timeout: str | None = None, statement_timeout: str | None = None) -> None:
    if lock_timeout:
        cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))
    if statement_timeout:
        cur.execute(sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(statement_timeout)))


def select_rows(cur, schema: str, table: str, columns: list[str], where: str | None = None):
    statement = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
        table_ident(schema, table),
    )
    if where:
        statement += sql.SQL(" WHERE ") + sql.SQL(where)
    cur.execute(statement)
    return cur
