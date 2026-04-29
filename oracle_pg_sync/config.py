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
    batch_size: int = 5000
    chunk_key: str | None = None
    sample_percent: float = 1.0


@dataclass
class ValidationConfig:
    checksum: ChecksumConfig = field(default_factory=ChecksumConfig)


@dataclass
class LobColumnConfig:
    strategy: str = "error"
    target_type: str | None = None
    validation: str | None = None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.strategy == other
        if isinstance(other, LobColumnConfig):
            return (
                self.strategy == other.strategy
                and self.target_type == other.target_type
                and self.validation == other.validation
            )
        return False


@dataclass
class LobStrategyConfig:
    default: str | None = None
    columns: dict[str, LobColumnConfig] = field(default_factory=dict)
    stream_batch_size: int = 100
    lob_chunk_size_bytes: int = 1024 * 1024
    validation: dict[str, Any] = field(default_factory=dict)
    warn_on_lob_larger_than_mb: int | None = 50
    fail_on_lob_larger_than_mb: int | None = None

    def __post_init__(self) -> None:
        self.columns = {str(key): _lob_column_config_from_raw(value) for key, value in self.columns.items()}


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
    truncate_resume_strategy: str = "restart_table"
    staging_schema: str | None = None


@dataclass
class ReportsConfig:
    output_dir: Path = Path("reports")


@dataclass
class DependencyConfig:
    auto_recompile_oracle: bool = True
    refresh_postgres_mview: bool = True
    max_recompile_attempts: int = 3
    fail_on_broken_dependency: bool = True


@dataclass
class JobConfig:
    retry: int = 3
    timeout_seconds: int = 3600
    alert_command: str = "echo FAILED"


@dataclass
class AppConfig:
    oracle: OracleConfig
    postgres: PostgresConfig
    sync: SyncConfig = field(default_factory=SyncConfig)
    reports: ReportsConfig = field(default_factory=ReportsConfig)
    dependency: DependencyConfig = field(default_factory=DependencyConfig)
    job: JobConfig = field(default_factory=JobConfig)
    tables: list[TableConfig] = field(default_factory=list)
    tables_file: Path | None = None
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
    dependency = DependencyConfig(**(raw.get("dependency") or {}))
    job = JobConfig(**(raw.get("job") or {}))
    tables_file = Path(raw["tables_file"]) if raw.get("tables_file") else None
    tables = _load_tables_config(raw, config_path, tables_file)
    rename_columns = {
        str(table).lower(): {str(k).lower(): str(v).lower() for k, v in mapping.items()}
        for table, mapping in (raw.get("rename_columns") or {}).items()
    }
    return AppConfig(
        oracle=oracle,
        postgres=postgres,
        sync=sync,
        reports=reports,
        dependency=dependency,
        job=job,
        tables=tables,
        tables_file=tables_file,
        rename_columns=rename_columns,
        validation=_validation_config_from_raw(raw.get("validation") or {}),
        lob_strategy=_lob_strategy_from_raw(raw.get("lob_strategy") or {}, default="error"),
    )


def _load_tables_config(raw: dict[str, Any], config_path: Path, tables_file: Path | None) -> list[TableConfig]:
    inline_tables = raw.get("tables") or []
    if inline_tables and tables_file:
        raise ValueError("Use either config.tables or tables_file, not both. Keep table lists in one place.")
    if inline_tables:
        return [_table_config_from_raw(t) for t in inline_tables]
    if not tables_file:
        return []
    path = tables_file
    if not path.is_absolute():
        path = config_path.parent / path
    table_raw = _load_raw_config(path)
    rows = table_raw.get("tables") if isinstance(table_raw, dict) else table_raw
    if not isinstance(rows, list):
        raise ValueError(f"tables_file must contain a list or a 'tables' list: {path}")
    return [_table_config_from_raw(t) for t in rows]


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
        str(key): _lob_column_config_from_raw(value)
        for key, value in (raw.get("columns") or {}).items()
    }
    return LobStrategyConfig(
        default=raw.get("default", default),
        columns=columns,
        stream_batch_size=int(raw.get("stream_batch_size", 100) or 100),
        lob_chunk_size_bytes=int(raw.get("lob_chunk_size_bytes", 1024 * 1024) or 1024 * 1024),
        validation=dict(raw.get("validation") or {}),
        warn_on_lob_larger_than_mb=raw.get("warn_on_lob_larger_than_mb", 50),
        fail_on_lob_larger_than_mb=raw.get("fail_on_lob_larger_than_mb"),
    )


def _lob_column_config_from_raw(value: Any) -> LobColumnConfig:
    if isinstance(value, LobColumnConfig):
        return value
    if isinstance(value, dict):
        return LobColumnConfig(
            strategy=str(value.get("strategy", "error")).lower(),
            target_type=str(value["target_type"]).lower() if value.get("target_type") else None,
            validation=str(value["validation"]).lower() if value.get("validation") else None,
        )
    return LobColumnConfig(strategy="null" if value is None else str(value).lower())


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
