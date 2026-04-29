from __future__ import annotations

import logging
from typing import Any

from oracle_pg_sync.config import AppConfig, TableConfig
from oracle_pg_sync.lob import is_lob_column, lob_type, resolve_lob_column_config, target_type_for_lob
from oracle_pg_sync.metadata.type_mapping import oracle_column, postgres_column
from oracle_pg_sync.utils.naming import split_schema_table


def analyze_lob_columns(config: AppConfig, tables: list[str], logger: logging.Logger) -> list[dict[str, Any]]:
    from oracle_pg_sync.db import oracle, postgres

    rows: list[dict[str, Any]] = []
    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            for table_name in tables:
                table = split_schema_table(table_name, config.postgres.schema)
                table_cfg = config.table_config(table.fqname) or TableConfig(name=table.fqname)
                logger.info("LOB analysis %s", table.fqname)
                oracle_columns = [oracle_column(row) for row in oracle.get_columns(ocur, config.oracle.schema, table.table)]
                postgres_columns = [postgres_column(row) for row in postgres.get_columns(pcur, table.schema, table.table)]
                rows.extend(_rows_for_source(config, table_cfg, table.fqname, "oracle", oracle_columns))
                rows.extend(_rows_for_source(config, table_cfg, table.fqname, "postgres", postgres_columns))
    return rows


def _rows_for_source(config: AppConfig, table_cfg: TableConfig, table_name: str, source_db: str, columns) -> list[dict[str, Any]]:
    total_columns = len(columns)
    lob_columns = [column for column in columns if is_lob_column(column)]
    classification = _classify_table(lob_columns, total_columns)
    if not lob_columns:
        return [
            {
                "source_db": source_db,
                "table_name": table_name,
                "classification": "normal",
                "column_name": "",
                "lob_type": "",
                "target_type": "",
                "strategy": "",
                "validation_mode": "",
                "warning": "",
                "suggestion": "No LOB column detected.",
            }
        ]
    rows: list[dict[str, Any]] = []
    for column in lob_columns:
        policy = resolve_lob_column_config(
            config,
            table_cfg,
            table_name=table_name,
            column_name=column.name,
        )
        type_name = lob_type(column)
        rows.append(
            {
                "source_db": source_db,
                "table_name": table_name,
                "classification": classification,
                "column_name": column.name,
                "lob_type": type_name,
                "target_type": target_type_for_lob(column, policy.target_type),
                "strategy": policy.strategy,
                "validation_mode": policy.validation or config.lob_strategy.validation.get("default", "size"),
                "warning": _warning(classification, lob_columns, total_columns, type_name),
                "suggestion": _suggestion(classification, policy.strategy),
            }
        )
    return rows


def _classify_table(lob_columns: list, total_columns: int) -> str:
    if not lob_columns:
        return "normal"
    binary_count = sum(1 for column in lob_columns if lob_type(column) in {"BLOB", "LONG RAW", "BYTEA"})
    if total_columns > 0 and len(lob_columns) / total_columns > 0.5:
        return "LOB-heavy"
    if binary_count >= max(1, len(lob_columns) // 2):
        return "binary-heavy"
    return "normal"


def _warning(classification: str, lob_columns: list, total_columns: int, type_name: str) -> str:
    if classification == "LOB-heavy" and total_columns:
        percent = round(len(lob_columns) * 100 / total_columns)
        return f"{percent}% LOB columns detected"
    if classification == "binary-heavy":
        return "Binary LOB columns detected"
    if type_name in {"LONG", "LONG RAW"}:
        return f"{type_name} support depends on Oracle driver fetch capability"
    return ""


def _suggestion(classification: str, strategy: str | None) -> str:
    if classification == "LOB-heavy":
        return "Review table scope; include selected LOB columns explicitly or exclude this table."
    if strategy in {None, "", "error"}:
        return "Configure lob_strategy column as skip, null, or stream before execute."
    if strategy in {"stream", "include"}:
        return "Stream sync enabled; validate with size or size_hash."
    return f"Strategy {strategy} is configured."
