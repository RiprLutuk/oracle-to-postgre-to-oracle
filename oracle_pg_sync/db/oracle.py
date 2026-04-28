from __future__ import annotations

from collections.abc import Iterable
import re
from typing import Any

import oracledb

from oracle_pg_sync.config import OracleConfig
from oracle_pg_sync.utils.naming import oracle_name


def init_client(config: OracleConfig) -> None:
    if not config.client_lib_dir:
        return
    try:
        oracledb.init_oracle_client(lib_dir=config.client_lib_dir)
    except Exception:
        # oracledb raises if the client was already initialized; that is harmless.
        pass


def connect(config: OracleConfig):
    init_client(config)
    con = oracledb.connect(
        user=config.user,
        password=config.password,
        dsn=config.resolved_dsn(),
    )
    con.autocommit = False
    return con


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def resolve_table_name(cur, owner: str, table: str) -> str | None:
    return _resolve_dictionary_name(cur, owner, table, dictionary="ALL_TABLES")


def resolve_table_or_view_name(cur, owner: str, table: str) -> str | None:
    return _resolve_dictionary_name(
        cur,
        owner,
        table,
        dictionary="ALL_OBJECTS",
        object_types=("TABLE", "VIEW", "MATERIALIZED VIEW"),
    )


def _clean_name(name: str) -> str:
    return name.strip().strip('"')


def _resolve_dictionary_name(
    cur,
    owner: str,
    name: str,
    *,
    dictionary: str,
    object_types: tuple[str, ...] = (),
) -> str | None:
    owner_u = owner.upper()
    raw_name = _clean_name(name)
    upper_name = oracle_name(name)
    if dictionary == "ALL_TABLES":
        exact_query = """
            SELECT TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER = :owner AND TABLE_NAME = :name
        """
        ci_query = """
            SELECT TABLE_NAME
            FROM ALL_TABLES
            WHERE OWNER = :owner AND LOWER(TABLE_NAME) = LOWER(:name)
            ORDER BY
                CASE
                    WHEN TABLE_NAME = :raw_name THEN 0
                    WHEN TABLE_NAME = :upper_name THEN 1
                    ELSE 2
                END,
                TABLE_NAME
        """
    elif dictionary == "ALL_OBJECTS":
        exact_query = """
            SELECT OBJECT_NAME
            FROM ALL_OBJECTS
            WHERE OWNER = :owner
              AND OBJECT_NAME = :name
              AND OBJECT_TYPE IN ({object_types})
        """.format(object_types=", ".join(f"'{item}'" for item in object_types))
        ci_query = """
            SELECT OBJECT_NAME
            FROM ALL_OBJECTS
            WHERE OWNER = :owner
              AND LOWER(OBJECT_NAME) = LOWER(:name)
              AND OBJECT_TYPE IN ({object_types})
            ORDER BY
                CASE
                    WHEN OBJECT_NAME = :raw_name THEN 0
                    WHEN OBJECT_NAME = :upper_name THEN 1
                    ELSE 2
                END,
                OBJECT_NAME
        """.format(object_types=", ".join(f"'{item}'" for item in object_types))
    else:
        raise ValueError(f"Unsupported dictionary: {dictionary}")

    cur.execute(exact_query, {"owner": owner_u, "name": upper_name})
    row = cur.fetchone()
    if row:
        return str(row[0])

    cur.execute(
        ci_query,
        {
            "owner": owner_u,
            "name": raw_name,
            "raw_name": raw_name,
            "upper_name": upper_name,
        },
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def _sql_table_name(cur, owner: str, table: str) -> str:
    return resolve_table_name(cur, owner, table) or oracle_name(table)


def table_exists(cur, owner: str, table: str) -> bool:
    return resolve_table_name(cur, owner, table) is not None


def table_or_view_exists(cur, owner: str, table: str) -> bool:
    return resolve_table_or_view_name(cur, owner, table) is not None


def count_rows(cur, owner: str, table: str) -> int:
    cur.execute(f"SELECT COUNT(1) FROM {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))}")
    return int(cur.fetchone()[0])


def min_max(cur, owner: str, table: str, column: str) -> tuple[Any, Any]:
    cur.execute(
        f"SELECT MIN({qident(column.upper())}), MAX({qident(column.upper())}) "
        f"FROM {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))}"
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def max_value(cur, owner: str, table: str, column: str, where: str | None = None) -> Any:
    query = (
        f"SELECT MAX({qident(column.upper())}) "
        f"FROM {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))}"
    )
    if where:
        query += f" WHERE {where}"
    cur.execute(query)
    row = cur.fetchone()
    return row[0] if row else None


def truncate_table(cur, owner: str, table: str) -> None:
    cur.execute(f"TRUNCATE TABLE {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))}")


def delete_rows(cur, owner: str, table: str) -> None:
    cur.execute(f"DELETE FROM {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))}")


def fast_count_rows(cur, owner: str, table: str) -> int | None:
    table_name = resolve_table_name(cur, owner, table)
    if not table_name:
        return None
    cur.execute(
        """
        SELECT NUM_ROWS
        FROM ALL_TABLES
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        """,
        {"owner": owner.upper(), "tbl": table_name},
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_columns(cur, owner: str, table: str) -> list[dict[str, Any]]:
    table_name = resolve_table_name(cur, owner, table)
    if not table_name:
        return []
    cur.execute(
        """
        SELECT COLUMN_NAME, COLUMN_ID, DATA_TYPE, DATA_LENGTH, CHAR_LENGTH,
               DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY COLUMN_ID
        """,
        {"owner": owner.upper(), "tbl": table_name},
    )
    columns: list[dict[str, Any]] = []
    for row in cur.fetchall():
        default_value = row[8]
        if hasattr(default_value, "read"):
            default_value = default_value.read()
        columns.append(
            {
                "name": row[0],
                "ordinal": row[1],
                "data_type": row[2],
                "data_length": row[3],
                "char_length": row[4],
                "numeric_precision": row[5],
                "numeric_scale": row[6],
                "nullable": row[7] == "Y",
                "default": default_value,
            }
        )
    return columns


def object_counts(cur, owner: str, table: str) -> dict[str, int]:
    owner_u = owner.upper()
    table_u = resolve_table_name(cur, owner, table) or oracle_name(table)
    counts: dict[str, int] = {}

    queries = {
        "index_count_oracle": (
            """
            SELECT COUNT(1)
            FROM ALL_INDEXES
            WHERE TABLE_OWNER = :owner AND TABLE_NAME = :tbl
            """,
            {"owner": owner_u, "tbl": table_u},
        ),
        "trigger_count_oracle": (
            """
            SELECT COUNT(1)
            FROM ALL_TRIGGERS
            WHERE TABLE_OWNER = :owner AND TABLE_NAME = :tbl
            """,
            {"owner": owner_u, "tbl": table_u},
        ),
        "constraint_count_oracle": (
            """
            SELECT COUNT(1)
            FROM ALL_CONSTRAINTS
            WHERE OWNER = :owner AND TABLE_NAME = :tbl
            """,
            {"owner": owner_u, "tbl": table_u},
        ),
        "sequence_count_oracle": (
            """
            SELECT COUNT(1)
            FROM ALL_SEQUENCES
            WHERE SEQUENCE_OWNER = :owner
              AND SEQUENCE_NAME LIKE '%' || :tbl || '%'
            """,
            {"owner": owner_u, "tbl": table_u},
        ),
        "view_count_related_oracle": (
            """
            SELECT COUNT(DISTINCT OWNER || '.' || NAME)
            FROM ALL_DEPENDENCIES
            WHERE REFERENCED_OWNER = :owner
              AND REFERENCED_NAME = :tbl
              AND TYPE = 'VIEW'
            """,
            {"owner": owner_u, "tbl": table_u},
        ),
        "stored_procedure_count_related_oracle": (
            """
            SELECT COUNT(DISTINCT OWNER || '.' || NAME)
            FROM ALL_DEPENDENCIES
            WHERE REFERENCED_OWNER = :owner
              AND REFERENCED_NAME = :tbl
              AND TYPE IN ('PROCEDURE', 'PACKAGE')
            """,
            {"owner": owner_u, "tbl": table_u},
        ),
    }

    for key, (query, binds) in queries.items():
        cur.execute(query, binds)
        counts[key] = int(cur.fetchone()[0] or 0)
    return counts


def dependency_rows(cur, owner: str, tables: Iterable[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    owner_u = owner.upper()
    for table in tables:
        table_u = resolve_table_name(cur, owner, table) or oracle_name(table)
        cur.execute(
            """
            SELECT OWNER, NAME, TYPE, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
            FROM ALL_DEPENDENCIES
            WHERE REFERENCED_OWNER = :owner AND REFERENCED_NAME = :tbl
            ORDER BY OWNER, TYPE, NAME
            """,
            {"owner": owner_u, "tbl": table_u},
        )
        for row in cur.fetchall():
            result.append(
                {
                    "source_db": "oracle",
                    "table_name": table,
                    "object_schema": row[0],
                    "object_name": row[1],
                    "object_type": row[2],
                    "referenced_schema": row[3],
                    "referenced_name": row[4],
                    "referenced_type": row[5],
                }
            )
    return result


def table_object_dependency_rows(cur, owner: str, table: str) -> list[dict[str, Any]]:
    rows = [
        {
            **row,
            "dependency_kind": "dictionary_dependency",
            "details": "",
        }
        for row in dependency_rows(cur, owner, [table])
    ]
    rows.extend(_index_rows(cur, owner, table))
    rows.extend(_trigger_rows(cur, owner, table))
    rows.extend(_sequence_rows(cur, owner, table))
    return _dedupe_dependency_rows(rows)


def _index_rows(cur, owner: str, table: str) -> list[dict[str, Any]]:
    owner_u = owner.upper()
    table_name = resolve_table_name(cur, owner, table) or oracle_name(table)
    cur.execute(
        """
        SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS, STATUS
        FROM ALL_INDEXES
        WHERE TABLE_OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY INDEX_NAME
        """,
        {"owner": owner_u, "tbl": table_name},
    )
    return [
        {
            "source_db": "oracle",
            "table_name": table,
            "object_schema": owner_u,
            "object_name": row[0],
            "object_type": "INDEX",
            "dependency_kind": "table_index",
            "referenced_schema": owner_u,
            "referenced_name": table_name,
            "referenced_type": "TABLE",
            "details": f"type={row[1]};unique={row[2]};status={row[3]}",
        }
        for row in cur.fetchall()
    ]


def _trigger_rows(cur, owner: str, table: str) -> list[dict[str, Any]]:
    owner_u = owner.upper()
    table_name = resolve_table_name(cur, owner, table) or oracle_name(table)
    cur.execute(
        """
        SELECT TRIGGER_NAME, STATUS, TRIGGERING_EVENT
        FROM ALL_TRIGGERS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY TRIGGER_NAME
        """,
        {"owner": owner_u, "tbl": table_name},
    )
    return [
        {
            "source_db": "oracle",
            "table_name": table,
            "object_schema": owner_u,
            "object_name": row[0],
            "object_type": "TRIGGER",
            "dependency_kind": "table_trigger",
            "referenced_schema": owner_u,
            "referenced_name": table_name,
            "referenced_type": "TABLE",
            "details": f"status={row[1]};event={row[2]}",
        }
        for row in cur.fetchall()
    ]


def _sequence_rows(cur, owner: str, table: str) -> list[dict[str, Any]]:
    owner_u = owner.upper()
    table_name = resolve_table_name(cur, owner, table) or oracle_name(table)
    rows: list[dict[str, Any]] = []
    rows.extend(_sequence_name_match_rows(cur, owner_u, table, table_name))
    rows.extend(_sequence_default_rows(cur, owner_u, table, table_name))
    rows.extend(_sequence_trigger_rows(cur, owner_u, table, table_name))
    return rows


def _sequence_name_match_rows(cur, owner: str, requested_table: str, table_name: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT SEQUENCE_NAME
        FROM ALL_SEQUENCES
        WHERE SEQUENCE_OWNER = :owner
          AND LOWER(SEQUENCE_NAME) LIKE '%' || LOWER(:tbl) || '%'
        ORDER BY SEQUENCE_NAME
        """,
        {"owner": owner, "tbl": table_name},
    )
    return [
        _sequence_dependency_row(
            requested_table,
            owner,
            sequence_name=row[0],
            table_name=table_name,
            kind="name_match",
            details="sequence name contains table name",
        )
        for row in cur.fetchall()
    ]


def _sequence_default_rows(cur, owner: str, requested_table: str, table_name: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_DEFAULT
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY COLUMN_ID
        """,
        {"owner": owner, "tbl": table_name},
    )
    rows: list[dict[str, Any]] = []
    for column_name, data_default in cur.fetchall():
        default_text = _read_lob_text(data_default)
        for sequence_name in _sequence_names_from_text(default_text):
            rows.append(
                _sequence_dependency_row(
                    requested_table,
                    owner,
                    sequence_name=sequence_name,
                    table_name=table_name,
                    kind="column_default_nextval",
                    details=f"column={column_name}",
                )
            )
    return rows


def _sequence_trigger_rows(cur, owner: str, requested_table: str, table_name: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT TRIGGER_NAME, TRIGGER_BODY
        FROM ALL_TRIGGERS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY TRIGGER_NAME
        """,
        {"owner": owner, "tbl": table_name},
    )
    rows: list[dict[str, Any]] = []
    for trigger_name, trigger_body in cur.fetchall():
        body = _read_lob_text(trigger_body)
        for sequence_name in _sequence_names_from_text(body):
            rows.append(
                _sequence_dependency_row(
                    requested_table,
                    owner,
                    sequence_name=sequence_name,
                    table_name=table_name,
                    kind="trigger_nextval",
                    details=f"trigger={trigger_name}",
                )
            )
    return rows


def _sequence_dependency_row(
    requested_table: str,
    owner: str,
    *,
    sequence_name: str,
    table_name: str,
    kind: str,
    details: str,
) -> dict[str, Any]:
    return {
        "source_db": "oracle",
        "table_name": requested_table,
        "object_schema": owner,
        "object_name": sequence_name,
        "object_type": "SEQUENCE",
        "dependency_kind": kind,
        "referenced_schema": owner,
        "referenced_name": table_name,
        "referenced_type": "TABLE",
        "details": details,
    }


def _sequence_names_from_text(text: str) -> list[str]:
    names: list[str] = []
    pattern = re.compile(r'(?:(?:"?[\w$#]+"?)\.)?("?[\w$#]+"?)\.NEXTVAL', re.IGNORECASE)
    for match in pattern.finditer(text or ""):
        name = match.group(1).strip('"')
        if name and name.upper() not in {item.upper() for item in names}:
            names.append(name.upper())
    return names


def _read_lob_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value.read() if hasattr(value, "read") else value)


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


def schema_object_rows(cur, owner: str, object_types: set[str]) -> list[dict[str, Any]]:
    owner_u = owner.upper()
    rows: list[dict[str, Any]] = []
    base_types = object_types - {"TRIGGER", "SEQUENCE"}
    if base_types:
        cur.execute(
            """
            SELECT OBJECT_TYPE, OBJECT_NAME, STATUS, LAST_DDL_TIME
            FROM ALL_OBJECTS
            WHERE OWNER = :owner
              AND OBJECT_TYPE IN (
                  'VIEW', 'MATERIALIZED VIEW', 'PROCEDURE', 'FUNCTION',
                  'PACKAGE', 'PACKAGE BODY', 'SYNONYM'
              )
            ORDER BY OBJECT_TYPE, OBJECT_NAME
            """,
            {"owner": owner_u},
        )
        for object_type, object_name, status, last_ddl_time in cur.fetchall():
            if object_type not in base_types:
                continue
            rows.append(
                {
                    "source_db": "oracle",
                    "object_schema": owner_u,
                    "object_type": object_type,
                    "object_name": object_name,
                    "parent_name": "",
                    "status": status,
                    "details": f"last_ddl_time={last_ddl_time}" if last_ddl_time else "",
                }
            )
    if "SEQUENCE" in object_types:
        cur.execute(
            """
            SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CYCLE_FLAG, ORDER_FLAG,
                   CACHE_SIZE, LAST_NUMBER
            FROM ALL_SEQUENCES
            WHERE SEQUENCE_OWNER = :owner
            ORDER BY SEQUENCE_NAME
            """,
            {"owner": owner_u},
        )
        for row in cur.fetchall():
            rows.append(
                {
                    "source_db": "oracle",
                    "object_schema": owner_u,
                    "object_type": "SEQUENCE",
                    "object_name": row[0],
                    "parent_name": "",
                    "status": "",
                    "details": (
                        f"min={row[1]};max={row[2]};increment={row[3]};cycle={row[4]};"
                        f"order={row[5]};cache={row[6]};last={row[7]}"
                    ),
                }
            )
    if "TRIGGER" in object_types:
        cur.execute(
            """
            SELECT TRIGGER_NAME, TABLE_NAME, STATUS, TRIGGERING_EVENT
            FROM ALL_TRIGGERS
            WHERE OWNER = :owner
            ORDER BY TABLE_NAME, TRIGGER_NAME
            """,
            {"owner": owner_u},
        )
        for trigger_name, table_name, status, event in cur.fetchall():
            rows.append(
                {
                    "source_db": "oracle",
                    "object_schema": owner_u,
                    "object_type": "TRIGGER",
                    "object_name": trigger_name,
                    "parent_name": table_name,
                    "status": status,
                    "details": f"event={event}",
                }
            )
    return rows


def select_rows(cur, owner: str, table: str, columns: list[tuple[str, str | None]], where: str | None = None):
    select_items = []
    for pg_column, oracle_column in columns:
        if oracle_column is None:
            select_items.append(f"CAST(NULL AS VARCHAR2(4000)) AS {qident(pg_column.upper())}")
        else:
            select_items.append(f"{qident(oracle_column.upper())} AS {qident(pg_column.upper())}")
    select_list = ", ".join(select_items)
    query = f"SELECT {select_list} FROM {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))}"
    if where:
        query += f" WHERE {where}"
    cur.execute(query)
    return cur


def insert_rows(
    cur,
    *,
    owner: str,
    table: str,
    oracle_columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0
    columns_sql = ", ".join(qident(col.upper()) for col in oracle_columns)
    binds_sql = ", ".join(f":{idx}" for idx in range(1, len(oracle_columns) + 1))
    cur.executemany(
        f"INSERT INTO {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))} ({columns_sql}) VALUES ({binds_sql})",
        rows,
    )
    return len(rows)


def merge_rows(
    cur,
    *,
    owner: str,
    table: str,
    oracle_columns: list[str],
    key_columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0
    key_set = {col.lower() for col in key_columns}
    update_columns = [col for col in oracle_columns if col.lower() not in key_set]
    if not update_columns:
        raise ValueError("upsert needs at least one non-key column to update")

    source_cols = ", ".join(
        f":{idx} AS {qident(col.upper())}"
        for idx, col in enumerate(oracle_columns, start=1)
    )
    on_clause = " AND ".join(
        f"t.{qident(col.upper())} = s.{qident(col.upper())}"
        for col in key_columns
    )
    update_clause = ", ".join(
        f"t.{qident(col.upper())} = s.{qident(col.upper())}"
        for col in update_columns
    )
    insert_cols = ", ".join(qident(col.upper()) for col in oracle_columns)
    insert_values = ", ".join(f"s.{qident(col.upper())}" for col in oracle_columns)
    statement = f"""
        MERGE INTO {qident(owner.upper())}.{qident(_sql_table_name(cur, owner, table))} t
        USING (SELECT {source_cols} FROM dual) s
        ON ({on_clause})
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """
    cur.executemany(statement, rows)
    return len(rows)
