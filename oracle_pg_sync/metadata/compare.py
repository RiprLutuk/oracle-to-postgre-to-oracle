from __future__ import annotations

from dataclasses import dataclass

from oracle_pg_sync.config import AppConfig
from oracle_pg_sync.metadata.oracle_metadata import OracleTableMetadata
from oracle_pg_sync.metadata.postgres_metadata import PostgresTableMetadata
from oracle_pg_sync.metadata.type_mapping import (
    ColumnMeta,
    is_type_compatible,
    oracle_type_label,
    pg_type_label,
    suggested_pg_type,
)
from oracle_pg_sync.utils.naming import split_schema_table


@dataclass
class AuditResult:
    inventory_rows: list[dict]
    column_diff_rows: list[dict]
    type_mismatch_rows: list[dict]
    dependency_rows: list[dict]


def compare_table_metadata(
    *,
    table_name: str,
    config: AppConfig,
    oracle_meta: OracleTableMetadata,
    postgres_meta: PostgresTableMetadata,
) -> tuple[dict, list[dict], list[dict]]:
    table = split_schema_table(table_name, config.postgres.schema)
    rename_map = config.rename_columns.get(table.key, {})

    oracle_cols = _mapped_oracle_columns(oracle_meta.columns, rename_map)
    pg_cols = {c.normalized_name: c for c in postgres_meta.columns}

    missing_in_pg = sorted(set(oracle_cols) - set(pg_cols))
    extra_in_pg = sorted(set(pg_cols) - set(oracle_cols))
    common = sorted(set(oracle_cols) & set(pg_cols), key=lambda name: oracle_cols[name].ordinal)

    column_diff_rows: list[dict] = []
    type_mismatch_rows: list[dict] = []

    for name in missing_in_pg:
        col = oracle_cols[name]
        column_diff_rows.append(
            {
                "table_name": table.fqname,
                "diff_type": "missing_in_postgres",
                "column_name": name,
                "oracle_type": oracle_type_label(col),
                "postgres_type": "",
                "suggested_pg_type": suggested_pg_type(col),
            }
        )
    for name in extra_in_pg:
        col = pg_cols[name]
        column_diff_rows.append(
            {
                "table_name": table.fqname,
                "diff_type": "extra_in_postgres",
                "column_name": name,
                "oracle_type": "",
                "postgres_type": pg_type_label(col),
                "suggested_pg_type": "",
            }
        )

    order_mismatch = False
    for name in common:
        oracle_col = oracle_cols[name]
        pg_col = pg_cols[name]
        if oracle_col.ordinal != pg_col.ordinal:
            order_mismatch = True
            column_diff_rows.append(
                {
                    "table_name": table.fqname,
                    "diff_type": "ordinal_mismatch",
                    "column_name": name,
                    "oracle_type": oracle_type_label(oracle_col),
                    "postgres_type": pg_type_label(pg_col),
                    "suggested_pg_type": "",
                    "oracle_ordinal": oracle_col.ordinal,
                    "postgres_ordinal": pg_col.ordinal,
                }
            )
        compatible, reason = is_type_compatible(oracle_col, pg_col)
        if not compatible:
            type_mismatch_rows.append(
                {
                    "table_name": table.fqname,
                    "column_name": name,
                    "oracle_type": oracle_type_label(oracle_col),
                    "postgres_type": pg_type_label(pg_col),
                    "reason": reason,
                }
            )

    row_count_match = (
        oracle_meta.row_count is not None
        and postgres_meta.row_count is not None
        and oracle_meta.row_count == postgres_meta.row_count
    )
    column_structure_match = not missing_in_pg and not extra_in_pg and not order_mismatch

    if not oracle_meta.exists or not postgres_meta.exists:
        status = "MISSING"
    elif type_mismatch_rows or missing_in_pg or extra_in_pg:
        status = "MISMATCH"
    elif not row_count_match:
        status = "WARNING"
    else:
        status = "MATCH"

    inventory = {
        "table_name": table.fqname,
        "oracle_exists": oracle_meta.exists,
        "postgres_exists": postgres_meta.exists,
        "oracle_row_count": oracle_meta.row_count,
        "postgres_row_count": postgres_meta.row_count,
        "row_count_match": row_count_match,
        "oracle_column_count": len(oracle_meta.columns),
        "postgres_column_count": len(postgres_meta.columns),
        "column_structure_match": column_structure_match,
        "type_mismatch_count": len(type_mismatch_rows),
        "missing_columns_in_pg": ";".join(missing_in_pg),
        "extra_columns_in_pg": ";".join(extra_in_pg),
        "index_count_oracle": oracle_meta.object_counts.get("index_count_oracle", 0),
        "index_count_postgres": postgres_meta.object_counts.get("index_count_postgres", 0),
        "view_count_related_oracle": oracle_meta.object_counts.get("view_count_related_oracle", 0),
        "view_count_related_postgres": postgres_meta.object_counts.get("view_count_related_postgres", 0),
        "sequence_count_oracle": oracle_meta.object_counts.get("sequence_count_oracle", 0),
        "sequence_count_postgres": postgres_meta.object_counts.get("sequence_count_postgres", 0),
        "stored_procedure_count_related_oracle": oracle_meta.object_counts.get(
            "stored_procedure_count_related_oracle", 0
        ),
        "function_count_related_postgres": postgres_meta.object_counts.get("function_count_related_postgres", 0),
        "trigger_count_oracle": oracle_meta.object_counts.get("trigger_count_oracle", 0),
        "trigger_count_postgres": postgres_meta.object_counts.get("trigger_count_postgres", 0),
        "constraint_count_oracle": oracle_meta.object_counts.get("constraint_count_oracle", 0),
        "constraint_count_postgres": postgres_meta.object_counts.get("constraint_count_postgres", 0),
        "status": status,
    }
    return inventory, column_diff_rows, type_mismatch_rows


def _mapped_oracle_columns(columns: list[ColumnMeta], rename_map: dict[str, str]) -> dict[str, ColumnMeta]:
    result: dict[str, ColumnMeta] = {}
    for col in columns:
        mapped_name = rename_map.get(col.normalized_name, col.normalized_name)
        result[mapped_name] = ColumnMeta(
            name=mapped_name,
            ordinal=col.ordinal,
            data_type=col.data_type,
            char_length=col.char_length,
            data_length=col.data_length,
            numeric_precision=col.numeric_precision,
            numeric_scale=col.numeric_scale,
            nullable=col.nullable,
            default=col.default,
            udt_name=col.udt_name,
        )
    return result


def inventory_has_fatal_mismatch(inventory_row: dict) -> bool:
    return (
        not inventory_row.get("oracle_exists")
        or not inventory_row.get("postgres_exists")
        or bool(inventory_row.get("missing_columns_in_pg"))
        or bool(inventory_row.get("extra_columns_in_pg"))
        or int(inventory_row.get("type_mismatch_count") or 0) > 0
    )
