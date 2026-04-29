from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableName:
    schema: str
    table: str

    @property
    def fqname(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def key(self) -> str:
        return self.fqname.lower()


def normalize_identifier(value: str) -> str:
    return value.strip().strip('"').lower()


def split_schema_table(name: str, default_schema: str = "public") -> TableName:
    parts = [p.strip().strip('"') for p in name.split(".") if p.strip()]
    if len(parts) == 2:
        return TableName(normalize_identifier(parts[0]), normalize_identifier(parts[1]))
    if len(parts) == 1:
        return TableName(normalize_identifier(default_schema), normalize_identifier(parts[0]))
    raise ValueError(f"Invalid table name: {name!r}")


def oracle_name(table: str) -> str:
    return table.strip().strip('"').upper()


def pg_staging_name(table: str, run_id: str) -> str:
    suffix = f"_{run_id}"
    prefix = "_stg_"
    max_table_len = max(1, 63 - len(prefix) - len(suffix))
    return f"{prefix}{table[:max_table_len]}{suffix}"


def pg_old_name(table: str, token: str, *, kind: str = "backup") -> str:
    suffix = f"__{kind}_{token}"
    return table[: max(1, 63 - len(suffix))] + suffix
