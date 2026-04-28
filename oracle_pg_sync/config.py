from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")
_SIZE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?I?B?)?\s*$", re.IGNORECASE)


@dataclass
class OracleConfig:
    dsn: str | None = None
    host: str | None = None
    port: int | str | None = None
    service_name: str | None = None
    sid: str | None = None
    user: str | None = None
    password: str | None = None
    schema: str = ""
    client_lib_dir: str | None = None

    def resolved_dsn(self) -> str:
        if self.dsn:
            return self.dsn
        if not self.host or not self.port:
            raise ValueError("Oracle DSN or host/port must be configured")
        import oracledb

        return oracledb.makedsn(
            self.host,
            int(self.port),
            service_name=self.service_name or None,
            sid=self.sid or None,
        )


@dataclass
class PostgresConfig:
    host: str | None = None
    port: int | str | None = 5432
    database: str | None = None
    user: str | None = None
    password: str | None = None
    schema: str = "public"

    def __post_init__(self) -> None:
        if not self.schema:
            self.schema = "public"

    def conninfo(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": int(self.port or 5432),
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


@dataclass
class IncrementalConfig:
    enabled: bool = False
    strategy: str = "updated_at"
    column: str | None = None
    initial_value: str | int | float | None = None
    overlap_minutes: int = 5
    delete_detection: bool = False


@dataclass
class ChecksumConfig:
    enabled: bool = False
    mode: str = "table"
    columns: str | list[str] = "auto"
    exclude_columns: list[str] = field(default_factory=list)
    sample_percent: float = 1.0


@dataclass
class ValidationConfig:
    checksum: ChecksumConfig = field(default_factory=ChecksumConfig)


@dataclass
class LobStrategyConfig:
    default: str | None = None
    columns: dict[str, str] = field(default_factory=dict)


@dataclass
class TableConfig:
    name: str
    source_schema: str | None = None
    source_table: str | None = None
    target_schema: str | None = None
    target_table: str | None = None
    mode: str | None = None
    oracle_to_postgres_mode: str | None = None
    postgres_to_oracle_mode: str | None = None
    directions: list[str] = field(default_factory=list)
    key_columns: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    where: str | None = None
    incremental: IncrementalConfig = field(default_factory=IncrementalConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    lob_strategy: LobStrategyConfig = field(default_factory=LobStrategyConfig)

    def __post_init__(self) -> None:
        if not self.name and self.target_table:
            target_schema = self.target_schema or "public"
            self.name = f"{target_schema}.{self.target_table}"
        if not self.key_columns and self.primary_key:
            self.key_columns = list(self.primary_key)


@dataclass
class SyncConfig:
    default_direction: str = "oracle-to-postgres"
    default_mode: str = "truncate"
    dry_run: bool = True
    fast_count: bool = True
    exact_count_after_load: bool = False
    parallel_workers: int = 1
    batch_size: int = 10000
    chunk_size: int = 50000
    skip_on_structure_mismatch: bool = True
    build_indexes_on_staging: bool = True
    analyze_after_load: bool = True
    truncate_cascade: bool = False
    allow_swap: bool = False
    max_swap_table_bytes: int | None = None
    swap_space_multiplier: float = 2.5
    keep_old_after_swap: bool = False
    copy_null: str = ""
    pg_lock_timeout: str = "5s"
    pg_statement_timeout: str = "0"
    checkpoint_dir: Path = Path("reports/checkpoints/checkpoint.sqlite3")


@dataclass
class ReportsConfig:
    output_dir: Path = Path("reports")


@dataclass
class AppConfig:
    oracle: OracleConfig
    postgres: PostgresConfig
    sync: SyncConfig = field(default_factory=SyncConfig)
    reports: ReportsConfig = field(default_factory=ReportsConfig)
    tables: list[TableConfig] = field(default_factory=list)
    rename_columns: dict[str, dict[str, str]] = field(default_factory=dict)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    lob_strategy: LobStrategyConfig = field(default_factory=lambda: LobStrategyConfig(default="error"))

    def table_names(self) -> list[str]:
        return [t.name for t in self.tables]

    def table_names_for_direction(self, direction: str) -> list[str]:
        result: list[str] = []
        for table in self.tables:
            directions = [item.lower() for item in table.directions]
            if not directions or direction in directions:
                result.append(table.name)
        return result

    def table_config(self, table_name: str) -> TableConfig | None:
        from oracle_pg_sync.utils.naming import split_schema_table

        key = split_schema_table(table_name, self.postgres.schema).key
        for table in self.tables:
            if split_schema_table(table.name, self.postgres.schema).key == key:
                return table
        return None


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            return os.getenv(name, default or "")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def _load_raw_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("PyYAML belum terinstall. Jalankan: pip install -r requirements.txt") from exc
    return yaml.safe_load(text) or {}


def load_config(path: str | Path = "config.yaml") -> AppConfig:
    config_path = Path(path)
    raw_pre_env = _load_raw_config(config_path)
    env_file = raw_pre_env.get("env_file")
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        load_dotenv = None
    if env_file:
        if load_dotenv:
            load_dotenv(config_path.parent / str(env_file))
    else:
        if load_dotenv:
            load_dotenv()

    raw = _expand_env(raw_pre_env)
    oracle = OracleConfig(**(raw.get("oracle") or {}))
    postgres = PostgresConfig(**(raw.get("postgres") or {}))
    sync_raw = raw.get("sync") or {}
    if "max_swap_table_bytes" in sync_raw:
        sync_raw["max_swap_table_bytes"] = _parse_size_bytes(sync_raw["max_swap_table_bytes"])
    if "checkpoint_dir" in sync_raw:
        sync_raw["checkpoint_dir"] = Path(sync_raw["checkpoint_dir"])
    sync = SyncConfig(**sync_raw)
    reports_raw = raw.get("reports") or {}
    reports = ReportsConfig(output_dir=Path(reports_raw.get("output_dir", "reports")))
    tables = [_table_config_from_raw(t) for t in raw.get("tables", [])]
    rename_columns = {
        str(table).lower(): {str(k).lower(): str(v).lower() for k, v in mapping.items()}
        for table, mapping in (raw.get("rename_columns") or {}).items()
    }
    return AppConfig(
        oracle=oracle,
        postgres=postgres,
        sync=sync,
        reports=reports,
        tables=tables,
        rename_columns=rename_columns,
        validation=_validation_config_from_raw(raw.get("validation") or {}),
        lob_strategy=_lob_strategy_from_raw(raw.get("lob_strategy") or {}, default="error"),
    )


def _table_config_from_raw(raw: Any) -> TableConfig:
    if not isinstance(raw, dict):
        return TableConfig(name=str(raw))
    item = dict(raw)
    item["incremental"] = _incremental_config_from_raw(item.get("incremental") or {})
    item["validation"] = _validation_config_from_raw(item.get("validation") or {})
    item["lob_strategy"] = _lob_strategy_from_raw(item.get("lob_strategy") or {}, default=None)
    item.setdefault("name", "")
    return TableConfig(**item)


def _incremental_config_from_raw(raw: dict[str, Any]) -> IncrementalConfig:
    return IncrementalConfig(**raw)


def _validation_config_from_raw(raw: dict[str, Any]) -> ValidationConfig:
    checksum_raw = raw.get("checksum") or {}
    return ValidationConfig(checksum=ChecksumConfig(**checksum_raw))


def _lob_strategy_from_raw(raw: dict[str, Any], *, default: str | None) -> LobStrategyConfig:
    columns = {
        str(key): ("null" if value is None else str(value))
        for key, value in (raw.get("columns") or {}).items()
    }
    return LobStrategyConfig(default=raw.get("default", default), columns=columns)


def mask_secret(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return value[:2] + "****" + value[-2:]


def _parse_size_bytes(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    match = _SIZE_PATTERN.match(str(value))
    if not match:
        raise ValueError(f"Invalid size value: {value!r}")
    amount = float(match.group(1))
    unit = (match.group(2) or "B").upper()
    multipliers = {
        "B": 1,
        "K": 1024,
        "KB": 1024,
        "KI": 1024,
        "KIB": 1024,
        "M": 1024**2,
        "MB": 1024**2,
        "MI": 1024**2,
        "MIB": 1024**2,
        "G": 1024**3,
        "GB": 1024**3,
        "GI": 1024**3,
        "GIB": 1024**3,
        "T": 1024**4,
        "TB": 1024**4,
        "TI": 1024**4,
        "TIB": 1024**4,
    }
    return int(amount * multipliers[unit])
