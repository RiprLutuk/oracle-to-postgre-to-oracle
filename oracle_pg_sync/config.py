from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-(.*?))?\}")


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
class TableConfig:
    name: str
    mode: str | None = None
    oracle_to_postgres_mode: str | None = None
    postgres_to_oracle_mode: str | None = None
    directions: list[str] = field(default_factory=list)
    key_columns: list[str] = field(default_factory=list)
    where: str | None = None


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
    keep_old_after_swap: bool = False
    copy_null: str = ""
    pg_lock_timeout: str = "5s"
    pg_statement_timeout: str = "0"


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
    sync = SyncConfig(**(raw.get("sync") or {}))
    reports_raw = raw.get("reports") or {}
    reports = ReportsConfig(output_dir=Path(reports_raw.get("output_dir", "reports")))
    tables = [
        TableConfig(**t) if isinstance(t, dict) else TableConfig(name=str(t))
        for t in raw.get("tables", [])
    ]
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
    )


def mask_secret(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return value[:2] + "****" + value[-2:]
