from __future__ import annotations

from collections.abc import Iterable
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


def table_exists(cur, owner: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM ALL_TABLES
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        """,
        {"owner": owner.upper(), "tbl": oracle_name(table)},
    )
    return cur.fetchone() is not None


def table_or_view_exists(cur, owner: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM ALL_OBJECTS
        WHERE OWNER = :owner
          AND OBJECT_NAME = :tbl
          AND OBJECT_TYPE IN ('TABLE', 'VIEW')
        """,
        {"owner": owner.upper(), "tbl": oracle_name(table)},
    )
    return cur.fetchone() is not None


def count_rows(cur, owner: str, table: str) -> int:
    cur.execute(
        f"SELECT COUNT(1) FROM {qident(owner.upper())}.{qident(oracle_name(table))}"
    )
    return int(cur.fetchone()[0])


def truncate_table(cur, owner: str, table: str) -> None:
    cur.execute(f"TRUNCATE TABLE {qident(owner.upper())}.{qident(oracle_name(table))}")


def delete_rows(cur, owner: str, table: str) -> None:
    cur.execute(f"DELETE FROM {qident(owner.upper())}.{qident(oracle_name(table))}")


def fast_count_rows(cur, owner: str, table: str) -> int | None:
    cur.execute(
        """
        SELECT NUM_ROWS
        FROM ALL_TABLES
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        """,
        {"owner": owner.upper(), "tbl": oracle_name(table)},
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_columns(cur, owner: str, table: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT COLUMN_NAME, COLUMN_ID, DATA_TYPE, DATA_LENGTH, CHAR_LENGTH,
               DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY COLUMN_ID
        """,
        {"owner": owner.upper(), "tbl": oracle_name(table)},
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
    table_u = oracle_name(table)
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
        table_u = oracle_name(table)
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


def select_rows(cur, owner: str, table: str, columns: list[tuple[str, str]], where: str | None = None):
    select_list = ", ".join(
        f"{qident(oracle_column.upper())} AS {qident(pg_column.upper())}"
        for pg_column, oracle_column in columns
    )
    query = f"SELECT {select_list} FROM {qident(owner.upper())}.{qident(oracle_name(table))}"
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
        f"INSERT INTO {qident(owner.upper())}.{qident(oracle_name(table))} ({columns_sql}) VALUES ({binds_sql})",
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
        MERGE INTO {qident(owner.upper())}.{qident(oracle_name(table))} t
        USING (SELECT {source_cols} FROM dual) s
        ON ({on_clause})
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """
    cur.executemany(statement, rows)
    return len(rows)
