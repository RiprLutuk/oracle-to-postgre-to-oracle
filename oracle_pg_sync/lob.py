from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from oracle_pg_sync.config import AppConfig, LobStrategyConfig, TableConfig
from oracle_pg_sync.metadata.type_mapping import ColumnMeta


LOB_TYPES = {"BLOB", "CLOB", "NCLOB", "LONG RAW", "BYTEA"}


@dataclass(frozen=True)
class LobDecision:
    column_name: str
    strategy: str
    reason: str = ""


def is_lob_column(column: ColumnMeta) -> bool:
    data_type = (column.data_type or "").upper()
    udt_name = (column.udt_name or "").upper()
    return data_type in LOB_TYPES or udt_name in LOB_TYPES or "LOB" in data_type


def resolve_lob_strategy(
    config: AppConfig,
    table_cfg: TableConfig,
    *,
    table_name: str,
    column_name: str,
) -> str:
    candidates = _column_strategy_candidates(table_name, column_name)
    for key in candidates:
        if key in table_cfg.lob_strategy.columns:
            return table_cfg.lob_strategy.columns[key].lower()
        if key in config.lob_strategy.columns:
            return config.lob_strategy.columns[key].lower()
    return (table_cfg.lob_strategy.default or config.lob_strategy.default or "error").lower()


def lob_decisions(
    config: AppConfig,
    table_cfg: TableConfig,
    *,
    table_name: str,
    source_columns: list[ColumnMeta],
) -> list[LobDecision]:
    decisions: list[LobDecision] = []
    for col in source_columns:
        if is_lob_column(col):
            strategy = resolve_lob_strategy(config, table_cfg, table_name=table_name, column_name=col.name)
            if strategy not in {"skip", "null", "stream", "error"}:
                raise ValueError(f"Unsupported LOB strategy {strategy!r} for {table_name}.{col.name}")
            decisions.append(LobDecision(col.normalized_name, strategy))
    return decisions


def apply_lob_mapping_policy(
    mapping: list[tuple[str, str]],
    *,
    config: AppConfig,
    table_cfg: TableConfig,
    table_name: str,
    source_columns: list[ColumnMeta],
) -> tuple[list[tuple[str, str | None]], dict[str, Any]]:
    by_name = {col.normalized_name: col for col in source_columns}
    filtered: list[tuple[str, str | None]] = []
    summary = {
        "lob_columns_detected": [],
        "lob_strategy_applied": {},
        "lob_columns_skipped": [],
        "lob_columns_nullified": [],
    }
    for target_col, source_col in mapping:
        col_meta = by_name.get(str(source_col).lower())
        if not col_meta or not is_lob_column(col_meta):
            filtered.append((target_col, source_col))
            continue
        strategy = resolve_lob_strategy(config, table_cfg, table_name=table_name, column_name=source_col)
        summary["lob_columns_detected"].append(target_col)
        summary["lob_strategy_applied"][target_col] = strategy
        if strategy == "skip":
            summary["lob_columns_skipped"].append(target_col)
            continue
        if strategy == "null":
            summary["lob_columns_nullified"].append(target_col)
            filtered.append((target_col, None))
            continue
        if strategy == "stream":
            filtered.append((target_col, source_col))
            continue
        raise ValueError(
            f"LOB column detected: {table_name}.{source_col}. "
            "Set lob_strategy default/column to skip, null, or stream before execute."
        )
    return filtered, summary


def _column_strategy_candidates(table_name: str, column_name: str) -> list[str]:
    column = column_name.strip().strip('"')
    table = table_name.strip().strip('"')
    parts = table.split(".")
    short = parts[-1]
    return [
        f"{table}.{column}",
        f"{table}.{column}".upper(),
        f"{short}.{column}",
        f"{short}.{column}".upper(),
        column,
        column.upper(),
        column.lower(),
    ]
