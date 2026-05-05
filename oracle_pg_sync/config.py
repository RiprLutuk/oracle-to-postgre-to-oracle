from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")
_SIZE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGT]?I?B?)?\s*$", re.IGNORECASE)
_LAST_ENV_FILE: Path | None = None
_LAST_ENV_LOADED = False
_OPTIONAL_ENV_DEFAULTS = {
    "ORACLE_DSN": "",
    "ORACLE_PORT": "1521",
    "ORACLE_SERVICE_NAME": "",
    "ORACLE_SID": "",
    "ORACLE_SCHEMA": "",
    "ORACLE_CLIENT_LIB_DIR": "",
    "PG_SCHEMA": "public",
}


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

    def conninfo_string(self) -> str:
        values = self.conninfo()
        try:
            from psycopg.conninfo import make_conninfo

            return make_conninfo(**values)
        except Exception:
            parts = []
            for key, value in values.items():
                if value is None:
                    continue
                text = str(value).replace("\\", "\\\\").replace("'", "\\'")
                parts.append(f"{key}='{text}'")
            return " ".join(parts)


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
    exclude_lob_by_default: bool = True


@dataclass
class ValidationConfig:
    checksum: ChecksumConfig = field(default_factory=ChecksumConfig)
    rowcount: "RowcountValidationConfig" = field(default_factory=lambda: RowcountValidationConfig())
    missing_keys: "MissingKeysConfig" = field(default_factory=lambda: MissingKeysConfig())


@dataclass
class RowcountValidationConfig:
    enabled: bool = True
    fail_on_mismatch: bool = True


@dataclass
class MissingKeysConfig:
    enabled: bool = True
    sample_limit: int = 1000


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
    bytea_format: str = "hex"
    clob_null_byte_policy: str = "remove"
    fail_on_lob_read_error: bool = True
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
    default_mode: str = "truncate_safe"
    dry_run: bool = True
    fast_count: bool = True
    exact_count_after_load: bool = False
    workers: int = 1
    parallel_tables: bool = False
    parallel_chunks: bool = False
    max_db_connections: int | None = None
    respect_dependencies: bool = False
    parallel_workers: int | None = None
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
    backup_before_truncate: bool = True
    backup_retention_count: int = 3
    staging_retention_count: int = 5
    max_failures: int = 3
    cooldown_minutes: int = 30
    skip_failed_rows: bool = False
    failed_row_sample_limit: int = 20
    skip_if_rowcount_match: bool = False

    def __post_init__(self) -> None:
        legacy_workers = int(self.parallel_workers or 0)
        configured_workers = int(self.workers or 0)
        if configured_workers <= 0 and legacy_workers > 0:
            configured_workers = legacy_workers
        self.workers = max(1, configured_workers or 1)
        self.parallel_workers = self.workers
        if self.max_db_connections is not None:
            self.max_db_connections = max(1, int(self.max_db_connections))


@dataclass
class ReportsConfig:
    output_dir: Path = Path("reports")


@dataclass
class DependencyConfig:
    auto_recompile_oracle: bool = True
    refresh_postgres_mview: bool = True
    max_recompile_attempts: int = 3
    max_attempts: int = 3
    fail_on_broken_dependency: bool = True


@dataclass
class EmailAlertConfig:
    from_address: str = ""
    to: list[str] = field(default_factory=list)
    smtp_host: str = ""
    smtp_port: int = 25
    username: str = ""
    password: str = ""
    use_tls: bool = True


@dataclass
class AlertConfig:
    type: str = ""
    url: str = ""
    on: list[str] = field(default_factory=lambda: ["failure", "repeated_failure", "dependency_error"])
    timeout_seconds: int = 10
    email: EmailAlertConfig = field(default_factory=EmailAlertConfig)


@dataclass
class JobConfig:
    name: str = "default"
    retry: int = 3
    timeout_seconds: int = 3600
    alert_command: str = "echo FAILED"
    alert: AlertConfig = field(default_factory=AlertConfig)


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
        return self.resolve_table_config(table_name, strict=False)

    def resolve_table_config(self, table_name: str, *, strict: bool = True) -> TableConfig | None:
        matches = self._table_resolution_matches(table_name)
        if not matches:
            if strict:
                raise ValueError(f"table not found in config: {table_name}")
            return None
        best_priority = min(priority for priority, _ in matches)
        best = [table for priority, table in matches if priority == best_priority]
        if len(best) > 1:
            choices = ", ".join(_table_mapping_label(table, self) for table in best)
            raise ValueError(f"Ambiguous table {table_name}; matches: {choices}")
        return best[0]

    def resolve_table_name(self, table_name: str, *, strict: bool = False) -> str:
        table = self.resolve_table_config(table_name, strict=strict)
        return table.name if table else table_name

    def _table_resolution_matches(self, table_name: str) -> list[tuple[int, TableConfig]]:
        from oracle_pg_sync.utils.naming import split_schema_table

        requested = str(table_name).strip()
        requested_l = requested.lower()
        requested_split = split_schema_table(requested, self.postgres.schema)
        requested_fq = requested_split.fqname.lower()
        requested_table = requested_split.table.lower()
        matches: list[tuple[int, TableConfig]] = []
        for table in self.tables:
            target_schema = table.target_schema or split_schema_table(table.name, self.postgres.schema).schema
            target_table = table.target_table or split_schema_table(table.name, self.postgres.schema).table
            source_schema = table.source_schema or self.oracle.schema
            source_table = table.source_table or target_table
            candidates = [
                (1, table.name.lower()),
                (2, f"{target_schema}.{target_table}".lower()),
                (3, f"{source_schema}.{source_table}".lower()),
                (4, target_table.lower()),
                (5, source_table.lower()),
            ]
            for priority, candidate in candidates:
                if requested_l == candidate or requested_fq == candidate or requested_table == candidate and "." not in candidate:
                    matches.append((priority, table))
                    break
        return matches


def load_environment(env_file: str | Path | None = None, *, config_path: Path | None = None) -> bool:
    global _LAST_ENV_FILE, _LAST_ENV_LOADED
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        load_dotenv = None

    if env_file:
        path = _resolve_env_file(Path(env_file), config_path=config_path)
        if not path.exists():
            raise RuntimeError(f"Environment file not found: {path}")
        loaded = load_dotenv(path, override=False) if load_dotenv else _load_simple_dotenv(path)
        _LAST_ENV_FILE = path
        _LAST_ENV_LOADED = bool(loaded) or path.exists()
        return _LAST_ENV_LOADED

    default_path = _resolve_default_env_file(config_path=config_path)
    if not default_path.exists():
        _LAST_ENV_FILE = default_path
        _LAST_ENV_LOADED = False
        return False
    loaded = load_dotenv(default_path, override=False) if load_dotenv else _load_simple_dotenv(default_path)
    _LAST_ENV_FILE = default_path
    _LAST_ENV_LOADED = bool(loaded) or default_path.exists()
    return _LAST_ENV_LOADED


def env_status() -> tuple[bool, str]:
    if _LAST_ENV_FILE:
        return _LAST_ENV_LOADED, str(_LAST_ENV_FILE)
    return _LAST_ENV_LOADED, ".env"


def _resolve_env_file(path: Path, *, config_path: Path | None = None) -> Path:
    if path.is_absolute():
        return path
    for base in _env_search_bases(config_path):
        candidate = base / path
        if candidate.exists():
            return candidate
    return _env_search_bases(config_path)[0] / path


def _resolve_default_env_file(*, config_path: Path | None = None) -> Path:
    for base in _env_search_bases(config_path):
        candidate = base / ".env"
        if candidate.exists():
            return candidate
    return _env_search_bases(config_path)[0] / ".env"


def _env_search_bases(config_path: Path | None = None) -> list[Path]:
    bases: list[Path] = []
    if config_path is not None:
        bases.append(config_path.expanduser().resolve().parent)
    bases.append(Path.cwd().resolve())
    if config_path is None:
        bases.append(Path(__file__).resolve().parents[1])

    unique: list[Path] = []
    for base in bases:
        if base not in unique:
            unique.append(base)
    return unique


def _load_simple_dotenv(path: Path) -> bool:
    loaded = False
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value
            loaded = True
    return loaded


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            if name in _OPTIONAL_ENV_DEFAULTS:
                return _OPTIONAL_ENV_DEFAULTS[name]
            raise RuntimeError(f"Environment variable {name} is not set. Check .env or export it.")

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


def load_config(path: str | Path = "config.yaml", *, env_file: str | Path | None = None) -> AppConfig:
    config_path = Path(path)
    raw_pre_env = _load_raw_config(config_path)
    configured_env_file = env_file if env_file is not None else raw_pre_env.get("env_file")
    load_environment(configured_env_file, config_path=config_path)

    raw = _expand_env(raw_pre_env)
    oracle = OracleConfig(**(raw.get("oracle") or {}))
    if oracle.client_lib_dir:
        client_lib_path = Path(oracle.client_lib_dir).expanduser()
        if not client_lib_path.is_absolute():
            client_lib_path = (config_path.parent / client_lib_path).resolve()
        oracle.client_lib_dir = str(client_lib_path)
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
    job = _job_config_from_raw(raw.get("job") or {})
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


def validate_oracle_config(config: OracleConfig) -> None:
    missing: list[str] = []
    if not config.dsn and not config.host:
        missing.append("ORACLE_HOST")
    if not config.user:
        missing.append("ORACLE_USER")
    if not config.password:
        missing.append("ORACLE_PASSWORD")
    if missing:
        raise RuntimeError(_missing_env_message(missing))


def validate_postgres_config(config: PostgresConfig) -> None:
    missing: list[str] = []
    if not config.host:
        missing.append("PG_HOST")
    if not config.port:
        missing.append("PG_PORT")
    if not config.database:
        missing.append("PG_DATABASE")
    if not config.user:
        missing.append("PG_USER")
    if not config.password:
        missing.append("PG_PASSWORD")
    if missing:
        raise RuntimeError(_missing_env_message(missing))


def missing_required_env_vars(config: AppConfig) -> list[str]:
    missing: list[str] = []
    if not config.oracle.dsn and not config.oracle.host:
        missing.append("ORACLE_HOST")
    if not config.oracle.user:
        missing.append("ORACLE_USER")
    if not config.oracle.password:
        missing.append("ORACLE_PASSWORD")
    if not config.postgres.host:
        missing.append("PG_HOST")
    if not config.postgres.port:
        missing.append("PG_PORT")
    if not config.postgres.database:
        missing.append("PG_DATABASE")
    if not config.postgres.user:
        missing.append("PG_USER")
    if not config.postgres.password:
        missing.append("PG_PASSWORD")
    return missing


def _missing_env_message(names: list[str]) -> str:
    if len(names) == 1:
        return f"Environment variable {names[0]} is not set. Check .env or export it."
    return f"Environment variables {', '.join(names)} are not set. Check .env or export them."


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
    rowcount_raw = raw.get("rowcount") or {}
    missing_keys_raw = raw.get("missing_keys") or {}
    return ValidationConfig(
        checksum=ChecksumConfig(**checksum_raw),
        rowcount=RowcountValidationConfig(**rowcount_raw),
        missing_keys=MissingKeysConfig(**missing_keys_raw),
    )


def _job_config_from_raw(raw: dict[str, Any]) -> JobConfig:
    item = dict(raw)
    alert_raw = item.get("alert") or {}
    email_raw = alert_raw.get("email") or {}
    item["alert"] = AlertConfig(
        type=str(alert_raw.get("type") or ""),
        url=str(alert_raw.get("url") or ""),
        on=[str(value) for value in (alert_raw.get("on") or ["failure", "repeated_failure", "dependency_error"])],
        timeout_seconds=int(alert_raw.get("timeout_seconds", 10) or 10),
        email=EmailAlertConfig(
            from_address=str(email_raw.get("from_address") or ""),
            to=[str(value) for value in (email_raw.get("to") or [])],
            smtp_host=str(email_raw.get("smtp_host") or ""),
            smtp_port=int(email_raw.get("smtp_port", 25) or 25),
            username=str(email_raw.get("username") or ""),
            password=str(email_raw.get("password") or ""),
            use_tls=bool(email_raw.get("use_tls", True)),
        ),
    )
    return JobConfig(**item)


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
        bytea_format=str(raw.get("bytea_format", "hex") or "hex"),
        clob_null_byte_policy=str(raw.get("clob_null_byte_policy", "remove") or "remove"),
        fail_on_lob_read_error=bool(raw.get("fail_on_lob_read_error", True)),
        validation=dict(raw.get("validation") or {}),
        warn_on_lob_larger_than_mb=raw.get("warn_on_lob_larger_than_mb", 50),
        fail_on_lob_larger_than_mb=raw.get("fail_on_lob_larger_than_mb"),
    )


def _table_mapping_label(table: TableConfig, config: AppConfig) -> str:
    from oracle_pg_sync.utils.naming import split_schema_table

    target = split_schema_table(table.name, config.postgres.schema)
    target_schema = table.target_schema or target.schema
    target_table = table.target_table or target.table
    source_schema = table.source_schema or config.oracle.schema
    source_table = table.source_table or target_table
    return f"{source_schema}.{source_table} -> {target_schema}.{target_table}"


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
