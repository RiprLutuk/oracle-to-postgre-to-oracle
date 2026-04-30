from __future__ import annotations

import re
from typing import Any

import psycopg
from psycopg import sql

from oracle_pg_sync.config import PostgresConfig, validate_postgres_config
from oracle_pg_sync.utils.retry import connect_retry

try:
    from psycopg_pool import ConnectionPool
except ImportError:
    ConnectionPool = None


def connect(config: PostgresConfig, *, autocommit: bool = False):
    validate_postgres_config(config)
    return connect_retry(
        lambda: psycopg.connect(**config.conninfo(), autocommit=autocommit),
        label=f"PostgreSQL connect host={config.host}",
    )


def connection_pool(
    config: PostgresConfig,
    *,
    min_size: int = 1,
    max_size: int = 1,
    timeout: int = 30,
):
    validate_postgres_config(config)
    if ConnectionPool is None:
        raise RuntimeError("psycopg_pool is required for PostgreSQL connection pooling")
    return connect_retry(
        lambda: ConnectionPool(
            conninfo=config.conninfo_string(),
            min_size=max(1, int(min_size or 1)),
            max_size=max(1, int(max_size or 1)),
            timeout=max(1, int(timeout or 30)),
        ),
        label=f"PostgreSQL pool connect host={config.host}",
    )


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


def count_rows_where(cur, schema: str, table: str, where: str | None = None) -> int:
    stmt = sql.SQL("SELECT COUNT(1) FROM {}").format(table_ident(schema, table))
    if where:
        stmt += sql.SQL(" WHERE ") + sql.SQL(where)
    cur.execute(stmt)
    return int(cur.fetchone()[0])


def min_max(cur, schema: str, table: str, column: str) -> tuple[Any, Any]:
    cur.execute(
        sql.SQL("SELECT MIN({col}), MAX({col}) FROM {}").format(
            table_ident(schema, table),
            col=sql.Identifier(column),
        )
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def max_value(cur, schema: str, table: str, column: str, where: str | None = None) -> Any:
    statement = sql.SQL("SELECT MAX({}) FROM {}").format(sql.Identifier(column), table_ident(schema, table))
    if where:
        statement += sql.SQL(" WHERE ") + sql.SQL(where)
    cur.execute(statement)
    row = cur.fetchone()
    return row[0] if row else None


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


def total_relation_size_bytes(cur, schema: str, table: str) -> int | None:
    cur.execute(
        """
        SELECT pg_total_relation_size(c.oid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind IN ('r', 'p', 'm')
        """,
        (schema, table),
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def list_matching_tables(cur, schema: str, pattern: str) -> list[str]:
    cur.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relkind IN ('r', 'p')
          AND c.relname LIKE %s
        ORDER BY c.relname DESC
        """,
        (schema, pattern),
    )
    return [str(row[0]) for row in cur.fetchall()]


def list_tables(cur, schema: str) -> list[str]:
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = %s
        ORDER BY tablename
        """,
        (schema,),
    )
    return [f"{schema}.{row[0]}" for row in cur.fetchall()]


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
    }
    counts: dict[str, int] = {}
    for key, (query, params) in queries.items():
        cur.execute(query, params)
        counts[key] = int(cur.fetchone()[0] or 0)
    counts["function_count_related_postgres"] = len(_function_dependency_rows(cur, schema, table))
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
    relkind_map = {"v": "VIEW", "m": "MATERIALIZED VIEW", "r": "TABLE", "p": "PARTITIONED TABLE"}
    return [
        {
            "source_db": "postgres",
            "table_name": f"{schema}.{table}",
            "object_schema": row[0],
            "object_name": row[1],
            "object_type": relkind_map.get(row[2], row[2]),
            "referenced_schema": row[3],
            "referenced_name": row[4],
            "referenced_type": relkind_map.get(row[5], row[5]),
        }
        for row in cur.fetchall()
    ]


def table_object_dependency_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    rows = [
        {
            **row,
            "dependency_kind": "rewrite_dependency",
            "details": "",
        }
        for row in dependency_rows(cur, schema, table)
    ]
    rows.extend(_index_rows(cur, schema, table))
    rows.extend(_function_dependency_rows(cur, schema, table))
    rows.extend(_trigger_rows(cur, schema, table))
    rows.extend(_sequence_rows(cur, schema, table))
    return _dedupe_dependency_rows(rows)


def _index_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        ORDER BY indexname
        """,
        (schema, table),
    )
    return [
        {
            "source_db": "postgres",
            "table_name": f"{schema}.{table}",
            "object_schema": schema,
            "object_name": row[0],
            "object_type": "INDEX",
            "dependency_kind": "table_index",
            "referenced_schema": schema,
            "referenced_name": table,
            "referenced_type": "TABLE",
            "details": row[1],
        }
        for row in cur.fetchall()
    ]


def _function_dependency_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    rows = _exact_function_dependency_rows(cur, schema, table)
    rows.extend(_heuristic_function_dependency_rows(cur, schema, table, existing=rows))
    return rows


def _exact_function_dependency_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT pn.nspname, p.proname, p.prokind::text, pg_get_function_identity_arguments(p.oid)
        FROM pg_depend d
        JOIN pg_proc p ON p.oid = d.objid
        JOIN pg_namespace pn ON pn.oid = p.pronamespace
        WHERE d.refobjid = (
            SELECT c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        )
        ORDER BY pn.nspname, p.proname
        """,
        (schema, table),
    )
    kind_map = {"f": "FUNCTION", "p": "PROCEDURE", "a": "AGGREGATE", "w": "WINDOW"}
    return [
        {
            "source_db": "postgres",
            "table_name": f"{schema}.{table}",
            "object_schema": row[0],
            "object_name": row[1],
            "object_type": kind_map.get(row[2], "FUNCTION"),
            "dependency_kind": "function_dependency",
            "referenced_schema": schema,
            "referenced_name": table,
            "referenced_type": "TABLE",
            "details": f"prokind={row[2]};args={row[3] or ''};source=pg_depend",
        }
        for row in cur.fetchall()
    ]


def _heuristic_function_dependency_rows(
    cur,
    schema: str,
    table: str,
    *,
    existing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen = {
        (
            str(row.get("object_schema") or "").lower(),
            str(row.get("object_name") or "").lower(),
            str(row.get("object_type") or "").upper(),
        )
        for row in existing
    }
    cur.execute(
        """
        SELECT pn.nspname, p.proname, p.prokind::text,
            pg_get_function_identity_arguments(p.oid),
            pg_get_functiondef(p.oid)
        FROM pg_proc p
        JOIN pg_namespace pn ON pn.oid = p.pronamespace
        WHERE pn.nspname NOT IN ('pg_catalog', 'information_schema')
        AND p.prokind IN ('f', 'p')
        AND pg_get_functiondef(p.oid) ILIKE %s
        ORDER BY pn.nspname, p.proname
        """,
        (f"%{table}%",),
    )
    matcher = _table_reference_pattern(schema, table)
    kind_map = {"f": "FUNCTION", "p": "PROCEDURE", "a": "AGGREGATE", "w": "WINDOW"}
    rows: list[dict[str, Any]] = []
    for object_schema, object_name, prokind, args, definition in cur.fetchall():
        object_type = kind_map.get(prokind, "FUNCTION")
        key = (str(object_schema).lower(), str(object_name).lower(), object_type)
        if key in seen:
            continue
        definition_text = str(definition or "")
        if not matcher.search(definition_text):
            continue
        rows.append(
            {
                "source_db": "postgres",
                "table_name": f"{schema}.{table}",
                "object_schema": object_schema,
                "object_name": object_name,
                "object_type": object_type,
                "dependency_kind": "function_definition_reference",
                "referenced_schema": schema,
                "referenced_name": table,
                "referenced_type": "TABLE",
                "details": f"prokind={prokind};args={args or ''};source=pg_get_functiondef heuristic",
            }
        )
    return rows


def _trigger_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT tg.tgname, p.proname, pn.nspname
        FROM pg_trigger tg
        LEFT JOIN pg_proc p ON p.oid = tg.tgfoid
        LEFT JOIN pg_namespace pn ON pn.oid = p.pronamespace
        JOIN pg_class c ON c.oid = tg.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND NOT tg.tgisinternal
        ORDER BY tg.tgname
        """,
        (schema, table),
    )
    return [
        {
            "source_db": "postgres",
            "table_name": f"{schema}.{table}",
            "object_schema": schema,
            "object_name": row[0],
            "object_type": "TRIGGER",
            "dependency_kind": "table_trigger",
            "referenced_schema": schema,
            "referenced_name": table,
            "referenced_type": "TABLE",
            "details": f"function={row[2]}.{row[1]}" if row[1] else "",
        }
        for row in cur.fetchall()
    ]


def _sequence_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_sequence_serial_rows(cur, schema, table))
    rows.extend(_sequence_name_match_rows(cur, schema, table))
    return rows


def _sequence_serial_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT a.attname, pg_get_serial_sequence(format('%%I.%%I', n.nspname, c.relname), a.attname)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )
    rows: list[dict[str, Any]] = []
    for column_name, sequence_fqname in cur.fetchall():
        if not sequence_fqname:
            continue
        seq_schema, seq_name = _split_pg_fqname(str(sequence_fqname), default_schema=schema)
        rows.append(
            {
                "source_db": "postgres",
                "table_name": f"{schema}.{table}",
                "object_schema": seq_schema,
                "object_name": seq_name,
                "object_type": "SEQUENCE",
                "dependency_kind": "serial_or_identity",
                "referenced_schema": schema,
                "referenced_name": table,
                "referenced_type": "TABLE",
                "details": f"column={column_name}",
            }
        )
    return rows


def _sequence_name_match_rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT schemaname, sequencename
        FROM pg_sequences
        WHERE schemaname = %s
          AND sequencename ILIKE '%%' || %s || '%%'
        ORDER BY schemaname, sequencename
        """,
        (schema, table),
    )
    return [
        {
            "source_db": "postgres",
            "table_name": f"{schema}.{table}",
            "object_schema": row[0],
            "object_name": row[1],
            "object_type": "SEQUENCE",
            "dependency_kind": "name_match",
            "referenced_schema": schema,
            "referenced_name": table,
            "referenced_type": "TABLE",
            "details": "sequence name contains table name",
        }
        for row in cur.fetchall()
    ]


def _split_pg_fqname(value: str, *, default_schema: str) -> tuple[str, str]:
    cleaned = value.replace('"', "")
    if "." not in cleaned:
        return default_schema, cleaned
    schema, name = cleaned.rsplit(".", 1)
    return schema, name


def _table_reference_pattern(schema: str, table: str) -> re.Pattern[str]:
    schema_re = re.escape(schema)
    table_re = re.escape(table)
    parts = [
        rf'(?<![A-Za-z0-9_]){schema_re}\s*\.\s*{table_re}(?![A-Za-z0-9_])',
        rf'(?<![A-Za-z0-9_])"{schema_re}"\s*\.\s*"{table_re}"(?![A-Za-z0-9_])',
        rf'(?<![A-Za-z0-9_]){table_re}(?![A-Za-z0-9_])',
        rf'(?<![A-Za-z0-9_])"{table_re}"(?![A-Za-z0-9_])',
    ]
    return re.compile("|".join(parts), re.IGNORECASE)


def _dedupe_dependency_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = (
            str(row.get("source_db", "")).lower(),
            str(row.get("table_name", "")).lower(),
            str(row.get("object_schema", "")).lower(),
            str(row.get("object_name", "")).lower(),
            str(row.get("object_type", "")).lower(),
            str(row.get("dependency_kind", "")).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def schema_object_rows(
    cur,
    schema: str,
    object_types: set[str],
    *,
    include_extension_objects: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    class_extension_filter = (
        ""
        if include_extension_objects
        else "AND NOT EXISTS (SELECT 1 FROM pg_depend dep WHERE dep.objid = c.oid AND dep.deptype = 'e')"
    )
    proc_extension_filter = (
        ""
        if include_extension_objects
        else "AND NOT EXISTS (SELECT 1 FROM pg_depend dep WHERE dep.objid = p.oid AND dep.deptype = 'e')"
    )
    trigger_extension_filter = (
        ""
        if include_extension_objects
        else "AND NOT EXISTS (SELECT 1 FROM pg_depend dep WHERE dep.objid = t.oid AND dep.deptype = 'e')"
    )
    if {"VIEW", "MATERIALIZED VIEW"} & object_types:
        cur.execute(
            f"""
            SELECT n.nspname, c.relname,
                   CASE c.relkind WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END AS object_type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind IN ('v', 'm')
            {class_extension_filter}
            ORDER BY object_type, c.relname
            """,
            (schema,),
        )
        for object_schema, object_name, object_type in cur.fetchall():
            if object_type in object_types:
                rows.append(
                    {
                        "source_db": "postgres",
                        "object_schema": object_schema,
                        "object_type": object_type,
                        "object_name": object_name,
                        "parent_name": "",
                        "status": "",
                        "details": "",
                    }
                )
    if "SEQUENCE" in object_types:
        cur.execute(
            f"""
            SELECT schemaname, sequencename, start_value, min_value, max_value,
                   increment_by, cycle, cache_size
            FROM pg_sequences
            WHERE schemaname = %s
              AND (
                  %s
                  OR NOT EXISTS (
                      SELECT 1
                      FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      JOIN pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'
                      WHERE n.nspname = pg_sequences.schemaname
                        AND c.relname = pg_sequences.sequencename
                  )
              )
            ORDER BY sequencename
            """,
            (schema, include_extension_objects),
        )
        for row in cur.fetchall():
            rows.append(
                {
                    "source_db": "postgres",
                    "object_schema": row[0],
                    "object_type": "SEQUENCE",
                    "object_name": row[1],
                    "parent_name": "",
                    "status": "",
                    "details": (
                        f"start={row[2]};min={row[3]};max={row[4]};increment={row[5]};"
                        f"cycle={row[6]};cache={row[7]}"
                    ),
                }
            )
    if {"FUNCTION", "PROCEDURE"} & object_types:
        cur.execute(
            f"""
            SELECT n.nspname, p.proname,
                   CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END AS object_type,
                   pg_get_function_identity_arguments(p.oid) AS args
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.prokind IN ('f', 'p')
            {proc_extension_filter}
            ORDER BY object_type, p.proname, args
            """,
            (schema,),
        )
        for object_schema, object_name, object_type, args in cur.fetchall():
            if object_type in object_types:
                rows.append(
                    {
                        "source_db": "postgres",
                        "object_schema": object_schema,
                        "object_type": object_type,
                        "object_name": object_name,
                        "parent_name": "",
                        "status": "",
                        "details": f"args={args or ''}",
                    }
                )
    if "TRIGGER" in object_types:
        cur.execute(
            f"""
            SELECT n.nspname, t.tgname, c.relname, pg_get_triggerdef(t.oid, true)
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND NOT t.tgisinternal
            {trigger_extension_filter}
            ORDER BY c.relname, t.tgname
            """,
            (schema,),
        )
        for object_schema, object_name, parent_name, trigger_def in cur.fetchall():
            rows.append(
                {
                    "source_db": "postgres",
                    "object_schema": object_schema,
                    "object_type": "TRIGGER",
                    "object_name": object_name,
                    "parent_name": parent_name,
                    "status": "",
                    "details": trigger_def,
                }
            )
    return rows


def refresh_materialized_views(cur, dependency_rows: list[dict[str, Any]], *, concurrently: bool = False) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []
    for row in dependency_rows:
        object_type = str(row.get("object_type") or "").upper()
        if object_type != "MATERIALIZED VIEW":
            continue
        schema = str(row.get("object_schema") or "")
        name = str(row.get("object_name") or "")
        if not schema or not name or (schema, name) in seen:
            continue
        seen.add((schema, name))
        statement = sql.SQL("REFRESH MATERIALIZED VIEW {}{}").format(
            sql.SQL("CONCURRENTLY ") if concurrently else sql.SQL(""),
            table_ident(schema, name),
        )
        try:
            cur.execute(statement)
            rows.append({
                "source_db": "postgres",
                "object_schema": schema,
                "object_type": "MATERIALIZED VIEW",
                "object_name": name,
                "maintenance_status": "refreshed",
                "error_message": "",
            })
        except Exception as exc:
            rows.append({
                "source_db": "postgres",
                "object_schema": schema,
                "object_type": "MATERIALIZED VIEW",
                "object_name": name,
                "maintenance_status": "failed",
                "error_message": str(exc),
            })
    return rows


def validate_dependent_objects(cur, dependency_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for row in dependency_rows:
        object_type = str(row.get("object_type") or "").upper()
        if object_type not in {"VIEW", "MATERIALIZED VIEW", "FUNCTION", "PROCEDURE", "TRIGGER"}:
            continue
        schema = str(row.get("object_schema") or "")
        name = str(row.get("object_name") or "")
        key = (schema.lower(), object_type, name.lower())
        if not schema or not name or key in seen:
            continue
        seen.add(key)
        exists = dependent_object_exists(cur, schema, name, object_type)
        rows.append({
            "source_db": "postgres",
            "object_schema": schema,
            "object_type": object_type,
            "object_name": name,
            "validation_status": "exists" if exists else "missing",
        })
    return rows


def dependent_object_exists(cur, schema: str, name: str, object_type: str) -> bool:
    relkind_by_type = {
        "VIEW": ("v",),
        "MATERIALIZED VIEW": ("m",),
    }
    if object_type in relkind_by_type:
        cur.execute(
            """
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind = ANY(%s)
            """,
            (schema, name, list(relkind_by_type[object_type])),
        )
        return cur.fetchone() is not None
    if object_type in {"FUNCTION", "PROCEDURE"}:
        cur.execute(
            """
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = %s
            """,
            (schema, name),
        )
        return cur.fetchone() is not None
    if object_type == "TRIGGER":
        cur.execute(
            """
            SELECT 1
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND t.tgname = %s
            """,
            (schema, name),
        )
        return cur.fetchone() is not None
    return False


def truncate_table(cur, schema: str, table: str, *, cascade: bool = False) -> None:
    stmt = sql.SQL("TRUNCATE TABLE {}{}").format(
        table_ident(schema, table),
        sql.SQL(" CASCADE") if cascade else sql.SQL(""),
    )
    cur.execute(stmt)


def drop_tables(cur, schema: str, tables: list[str]) -> None:
    for table in tables:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(schema, table)))


def insert_from_table(
    cur,
    *,
    target_schema: str,
    target_table: str,
    source_schema: str,
    source_table: str,
    columns: list[str],
) -> None:
    cols = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
    stmt = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
        table_ident(target_schema, target_table),
        cols,
        cols,
        table_ident(source_schema, source_table),
    )
    cur.execute(stmt)


def analyze_table(cur, schema: str, table: str) -> None:
    cur.execute(sql.SQL("ANALYZE {}").format(table_ident(schema, table)))


def set_local_timeouts(cur, *, lock_timeout: str | None = None, statement_timeout: str | None = None) -> None:
    if lock_timeout:
        cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))
    if statement_timeout:
        cur.execute(sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(statement_timeout)))


def select_rows(
    cur,
    schema: str,
    table: str,
    columns: list[str | None],
    where: str | None = None,
    order_by: list[str] | None = None,
):
    select_items = [sql.SQL("NULL") if col is None else sql.Identifier(col) for col in columns]
    statement = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(select_items),
        table_ident(schema, table),
    )
    if where:
        statement += sql.SQL(" WHERE ") + sql.SQL(where)
    if order_by:
        statement += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(sql.Identifier(column) for column in order_by)
    cur.execute(statement)
    return cur
