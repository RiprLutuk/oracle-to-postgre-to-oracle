from __future__ import annotations

from dataclasses import dataclass


DEFAULT_OBJECT_TYPES = [
    "VIEW",
    "MATERIALIZED VIEW",
    "SEQUENCE",
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "PACKAGE BODY",
    "TRIGGER",
    "SYNONYM",
]


@dataclass
class ObjectAuditResult:
    inventory_rows: list[dict]
    compare_rows: list[dict]


def compare_object_inventory(oracle_rows: list[dict], postgres_rows: list[dict]) -> list[dict]:
    oracle_by_key = {_object_key(row): row for row in oracle_rows}
    postgres_by_key = {_object_key(row): row for row in postgres_rows}
    keys = sorted(set(oracle_by_key) | set(postgres_by_key))
    rows: list[dict] = []
    for key in keys:
        oracle_row = oracle_by_key.get(key)
        postgres_row = postgres_by_key.get(key)
        if oracle_row and postgres_row:
            status = "MATCH"
        elif oracle_row:
            status = "MISSING_IN_POSTGRES"
        else:
            status = "MISSING_IN_ORACLE"
        source = oracle_row or postgres_row or {}
        rows.append(
            {
                "object_type": source.get("object_type", ""),
                "object_name": source.get("object_name", ""),
                "parent_name": source.get("parent_name", ""),
                "oracle_exists": bool(oracle_row),
                "postgres_exists": bool(postgres_row),
                "status": status,
                "oracle_status": (oracle_row or {}).get("status", ""),
                "postgres_status": (postgres_row or {}).get("status", ""),
                "oracle_details": (oracle_row or {}).get("details", ""),
                "postgres_details": (postgres_row or {}).get("details", ""),
            }
        )
    return rows


def normalize_object_types(values: list[str] | None) -> set[str]:
    if not values:
        return set(DEFAULT_OBJECT_TYPES)
    aliases = {
        "MVIEW": "MATERIALIZED VIEW",
        "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
        "PROC": "PROCEDURE",
        "SP": "PROCEDURE",
        "PKG": "PACKAGE",
    }
    return {aliases.get(value.upper().replace("-", "_"), value.upper().replace("_", " ")) for value in values}


def _object_key(row: dict) -> tuple[str, str, str]:
    return (
        str(row.get("object_type") or "").lower(),
        str(row.get("parent_name") or "").lower(),
        str(row.get("object_name") or "").lower(),
    )
