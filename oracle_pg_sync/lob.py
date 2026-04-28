from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from oracle_pg_sync.config import AppConfig, LobColumnConfig, LobStrategyConfig, TableConfig
from oracle_pg_sync.metadata.type_mapping import ColumnMeta


ORACLE_LOB_TYPES = {"BLOB", "CLOB", "NCLOB", "LONG", "LONG RAW"}
BINARY_LOB_TYPES = {"BLOB", "LONG RAW", "BYTEA"}
TEXT_LOB_TYPES = {"CLOB", "NCLOB", "LONG"}
POSTGRES_LOB_TYPES = {"BYTEA"}
LOB_TYPES = ORACLE_LOB_TYPES | POSTGRES_LOB_TYPES
SUPPORTED_LOB_STRATEGIES = {"skip", "null", "stream", "include", "error"}


@dataclass(frozen=True)
class LobDecision:
    column_name: str
    strategy: str
    reason: str = ""


@dataclass(frozen=True)
class LobColumnPolicy:
    column_name: str
    strategy: str
    source_type: str
    lob_type: str
    target_type: str
    validation_mode: str


def is_lob_column(column: ColumnMeta) -> bool:
    data_type = (column.data_type or "").upper()
    udt_name = (column.udt_name or "").upper()
    return (
        data_type in LOB_TYPES
        or udt_name in LOB_TYPES
        or "LOB" in data_type
        or data_type == "LONG"
        or udt_name == "BYTEA"
    )


def lob_type(column: ColumnMeta) -> str:
    data_type = (column.data_type or "").upper()
    udt_name = (column.udt_name or "").upper()
    if data_type in LOB_TYPES:
        return data_type
    if udt_name in LOB_TYPES:
        return udt_name
    if "NCLOB" in data_type:
        return "NCLOB"
    if "CLOB" in data_type:
        return "CLOB"
    if "BLOB" in data_type:
        return "BLOB"
    if data_type == "LONG":
        return "LONG"
    return data_type or udt_name


def target_type_for_lob(column: ColumnMeta, explicit: str | None = None) -> str:
    if explicit:
        return explicit.lower()
    return "bytea" if lob_type(column) in BINARY_LOB_TYPES else "text"


def resolve_lob_column_config(
    config: AppConfig,
    table_cfg: TableConfig,
    *,
    table_name: str,
    column_name: str,
) -> LobColumnConfig:
    candidates = _column_strategy_candidates(table_name, column_name)
    for key in candidates:
        if key in table_cfg.lob_strategy.columns:
            return _coerce_lob_column_config(table_cfg.lob_strategy.columns[key])
        if key in config.lob_strategy.columns:
            return _coerce_lob_column_config(config.lob_strategy.columns[key])
    return LobColumnConfig(strategy=(table_cfg.lob_strategy.default or config.lob_strategy.default or "error").lower())


def resolve_lob_strategy(
    config: AppConfig,
    table_cfg: TableConfig,
    *,
    table_name: str,
    column_name: str,
) -> str:
    return resolve_lob_column_config(
        config,
        table_cfg,
        table_name=table_name,
        column_name=column_name,
    ).strategy.lower()


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
            if strategy not in SUPPORTED_LOB_STRATEGIES:
                raise ValueError(f"Unsupported LOB strategy {strategy!r} for {table_name}.{col.name}")
            decisions.append(LobDecision(col.normalized_name, _normalize_strategy(strategy)))
    return decisions


def apply_lob_mapping_policy(
    mapping: list[tuple[str, str]],
    *,
    config: AppConfig,
    table_cfg: TableConfig,
    table_name: str,
    source_columns: list[ColumnMeta],
    policy_column_side: str = "source",
) -> tuple[list[tuple[str, str | None]], dict[str, Any]]:
    by_name = {col.normalized_name: col for col in source_columns}
    filtered: list[tuple[str, str | None]] = []
    summary = {
        "lob_columns_detected": [],
        "lob_columns_synced": [],
        "lob_strategy_applied": {},
        "lob_columns_skipped": [],
        "lob_columns_nullified": [],
        "lob_type": {},
        "lob_target_type": {},
        "lob_validation_mode": {},
    }
    for target_col, source_col in mapping:
        policy_name = target_col if policy_column_side == "target" else source_col
        col_meta = by_name.get(str(policy_name).lower()) if policy_name is not None else None
        if not col_meta or not is_lob_column(col_meta):
            filtered.append((target_col, source_col))
            continue
        column_config = resolve_lob_column_config(config, table_cfg, table_name=table_name, column_name=str(policy_name))
        strategy = _normalize_strategy(column_config.strategy)
        if strategy not in SUPPORTED_LOB_STRATEGIES:
            raise ValueError(f"Unsupported LOB strategy {strategy!r} for {table_name}.{policy_name}")
        detected_name = str(target_col)
        type_name = lob_type(col_meta)
        target_type = target_type_for_lob(col_meta, column_config.target_type)
        validation_mode = column_config.validation or _default_lob_validation(config.lob_strategy)
        summary["lob_columns_detected"].append(detected_name)
        summary["lob_strategy_applied"][detected_name] = strategy
        summary["lob_type"][detected_name] = type_name
        summary["lob_target_type"][detected_name] = target_type
        summary["lob_validation_mode"][detected_name] = validation_mode
        if strategy == "skip":
            summary["lob_columns_skipped"].append(detected_name)
            continue
        if strategy == "null":
            summary["lob_columns_nullified"].append(detected_name)
            filtered.append((target_col, None))
            continue
        if strategy in {"stream", "include"}:
            summary["lob_columns_synced"].append(detected_name)
            filtered.append((target_col, source_col))
            continue
        raise ValueError(
            f"LOB column detected: {table_name}.{policy_name} ({type_name}). "
            "Set lob_strategy default/column to skip, null, stream/include, or keep error intentionally."
        )
    return filtered, summary


def lob_summary_to_fields(summary: dict[str, Any]) -> dict[str, str]:
    return {
        "lob_columns_detected": ";".join(summary.get("lob_columns_detected") or []),
        "lob_columns_synced": ";".join(summary.get("lob_columns_synced") or []),
        "lob_strategy_applied": _join_map(summary.get("lob_strategy_applied") or {}),
        "lob_columns_skipped": ";".join(summary.get("lob_columns_skipped") or []),
        "lob_columns_nullified": ";".join(summary.get("lob_columns_nullified") or []),
        "lob_type": _join_map(summary.get("lob_type") or {}),
        "lob_target_type": _join_map(summary.get("lob_target_type") or {}),
        "lob_validation_mode": _join_map(summary.get("lob_validation_mode") or {}),
    }


def oracle_lob_validation_expressions(column_name: str, column: ColumnMeta, *, hash_algorithm: str = "sha256") -> dict[str, str]:
    qcol = '"' + column_name.upper().replace('"', '""') + '"'
    type_name = lob_type(column)
    if type_name in {"LONG", "LONG RAW"}:
        return {
            "size": f"LENGTH({qcol})",
            "hash": "",
            "hash_validation_status": f"skipped_with_reason: Oracle {type_name} cannot be hashed safely in SQL",
        }
    if type_name in BINARY_LOB_TYPES:
        return {
            "size": f"DBMS_LOB.GETLENGTH({qcol})",
            "hash": f"RAWTOHEX(DBMS_CRYPTO.HASH({qcol}, {_oracle_hash_algorithm_id(hash_algorithm)}))",
            "hash_validation_status": "available",
        }
    return {
        "size": f"DBMS_LOB.GETLENGTH({qcol})",
        "hash": "",
        "hash_validation_status": "skipped_with_reason: text LOB hash requires safe driver chunk hashing",
    }


def postgres_lob_validation_expressions(column_name: str, column: ColumnMeta, *, hash_algorithm: str = "sha256") -> dict[str, str]:
    qcol = '"' + column_name.replace('"', '""') + '"'
    type_name = lob_type(column)
    if type_name in BINARY_LOB_TYPES:
        return {
            "size": f"octet_length({qcol})",
            "hash": f"encode(digest({qcol}, '{hash_algorithm.lower()}'), 'hex')",
            "hash_validation_status": "available",
        }
    return {
        "size": f"length({qcol})",
        "hash": f"encode(digest({qcol}, '{hash_algorithm.lower()}'), 'hex')",
        "hash_validation_status": "available",
    }


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


def _coerce_lob_column_config(value: LobColumnConfig | str) -> LobColumnConfig:
    if isinstance(value, LobColumnConfig):
        return value
    return LobColumnConfig(strategy="null" if value is None else str(value).lower())


def _normalize_strategy(strategy: str | None) -> str:
    value = (strategy or "error").lower()
    return "stream" if value == "include" else value


def _default_lob_validation(config: LobStrategyConfig) -> str:
    raw = config.validation or {}
    return str(raw.get("default") or "size").lower()


def _join_map(values: dict[str, Any]) -> str:
    return ";".join(f"{key}:{value}" for key, value in values.items())


def _oracle_hash_algorithm_id(hash_algorithm: str) -> int:
    algorithms = {
        "md5": 1,
        "sha1": 3,
        "sha256": 4,
        "sha384": 5,
        "sha512": 6,
    }
    return algorithms.get(hash_algorithm.lower(), 4)
