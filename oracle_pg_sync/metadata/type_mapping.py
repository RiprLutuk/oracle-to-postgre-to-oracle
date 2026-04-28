from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    ordinal: int
    data_type: str
    char_length: int | None = None
    data_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    nullable: bool = True
    default: str | None = None
    udt_name: str | None = None

    @property
    def normalized_name(self) -> str:
        return self.name.lower()


def oracle_column(row: dict) -> ColumnMeta:
    return ColumnMeta(
        name=str(row["name"]),
        ordinal=int(row["ordinal"] or 0),
        data_type=str(row.get("data_type") or ""),
        data_length=row.get("data_length"),
        char_length=row.get("char_length"),
        numeric_precision=row.get("numeric_precision"),
        numeric_scale=row.get("numeric_scale"),
        nullable=bool(row.get("nullable")),
        default=str(row["default"]).strip() if row.get("default") is not None else None,
    )


def postgres_column(row: dict) -> ColumnMeta:
    return ColumnMeta(
        name=str(row["name"]),
        ordinal=int(row["ordinal"] or 0),
        data_type=str(row.get("data_type") or ""),
        char_length=row.get("char_length"),
        numeric_precision=row.get("numeric_precision"),
        numeric_scale=row.get("numeric_scale"),
        nullable=bool(row.get("nullable")),
        default=str(row["default"]).strip() if row.get("default") is not None else None,
        udt_name=str(row["udt_name"]) if row.get("udt_name") else None,
    )


def pg_type_label(col: ColumnMeta) -> str:
    dt = col.data_type.lower()
    udt = (col.udt_name or "").lower()
    if dt in {"character varying", "varchar"}:
        return f"varchar({col.char_length})" if col.char_length else "varchar"
    if dt in {"character", "char"} or udt == "bpchar":
        return f"char({col.char_length})" if col.char_length else "char"
    if dt in {"numeric", "decimal"}:
        if col.numeric_precision is not None and col.numeric_scale is not None:
            return f"numeric({col.numeric_precision},{col.numeric_scale})"
        return "numeric"
    return udt or dt


def oracle_type_label(col: ColumnMeta) -> str:
    dt = col.data_type.upper()
    if dt in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        length = col.char_length or col.data_length
        return f"{dt}({length})" if length else dt
    if dt == "NUMBER":
        if col.numeric_precision is not None and col.numeric_scale is not None:
            return f"NUMBER({col.numeric_precision},{col.numeric_scale})"
        if col.numeric_precision is not None:
            return f"NUMBER({col.numeric_precision})"
    return dt


def is_type_compatible(oracle: ColumnMeta, postgres: ColumnMeta) -> tuple[bool, str]:
    odt = oracle.data_type.upper()
    pdt = pg_type_label(postgres).upper()

    if odt in {"VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        if any(token in pdt for token in ("VARCHAR", "CHAR", "TEXT", "BPCHAR")):
            pg_len = _extract_length(pdt)
            ora_len = oracle.char_length or oracle.data_length
            if pg_len is not None and ora_len is not None and pg_len < ora_len:
                return False, f"length Oracle {ora_len} > PostgreSQL {pg_len}"
            return True, ""
        return False, f"Oracle {oracle_type_label(oracle)} vs PostgreSQL {pg_type_label(postgres)}"

    if odt == "NUMBER":
        if any(token in pdt for token in ("NUMERIC", "DECIMAL")):
            pg_precision, pg_scale = _extract_precision_scale(pdt)
            if (
                pg_precision is not None
                and oracle.numeric_precision is not None
                and pg_precision < oracle.numeric_precision
            ):
                return False, f"precision Oracle {oracle.numeric_precision} > PostgreSQL {pg_precision}"
            if pg_scale is not None and oracle.numeric_scale is not None and pg_scale < oracle.numeric_scale:
                return False, f"scale Oracle {oracle.numeric_scale} > PostgreSQL {pg_scale}"
            return True, ""
        if any(token in pdt for token in ("SMALLINT", "INTEGER", "INT", "BIGINT")):
            if oracle.numeric_scale not in (None, 0):
                return False, f"Oracle scale {oracle.numeric_scale} cannot fit integer"
            precision = oracle.numeric_precision
            if precision is None:
                return True, ""
            if ("SMALLINT" in pdt or pdt == "INT2") and precision <= 4:
                return True, ""
            if ("INTEGER" in pdt or pdt in {"INT", "INT4"}) and precision <= 9:
                return True, ""
            if ("BIGINT" in pdt or pdt == "INT8") and precision <= 18:
                return True, ""
            return False, f"Oracle NUMBER({precision},0) too large for {pg_type_label(postgres)}"
        if any(token in pdt for token in ("DOUBLE", "REAL", "FLOAT")):
            return True, ""
        return False, f"Oracle {oracle_type_label(oracle)} vs PostgreSQL {pg_type_label(postgres)}"

    if odt in {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}:
        if any(token in pdt for token in ("NUMERIC", "DECIMAL", "DOUBLE", "REAL", "FLOAT")):
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt == "DATE" or odt.startswith("TIMESTAMP"):
        if any(token in pdt for token in ("DATE", "TIMESTAMP", "TIME")):
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt.startswith("INTERVAL"):
        if "INTERVAL" in pdt:
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt == "BOOLEAN":
        if "BOOL" in pdt:
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt in {"RAW", "BLOB", "LONG RAW"}:
        if "BYTEA" in pdt:
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if "CLOB" in odt or odt == "LONG":
        if any(token in pdt for token in ("TEXT", "VARCHAR")):
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt in {"ROWID", "UROWID"}:
        if any(token in pdt for token in ("TEXT", "VARCHAR", "CHAR")):
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt in {"JSON", "XMLTYPE"}:
        if any(token in pdt for token in ("JSON", "JSONB", "TEXT", "VARCHAR")):
            return True, ""
        return False, f"Oracle {odt} vs PostgreSQL {pg_type_label(postgres)}"

    if odt == pdt:
        return True, ""
    return False, f"Oracle {oracle_type_label(oracle)} vs PostgreSQL {pg_type_label(postgres)}"


def suggested_pg_type(oracle: ColumnMeta) -> str:
    odt = oracle.data_type.upper()
    if odt in {"VARCHAR", "VARCHAR2", "NVARCHAR2"}:
        length = oracle.char_length or oracle.data_length
        return f"varchar({length})" if length else "varchar"
    if odt in {"CHAR", "NCHAR"}:
        length = oracle.char_length or oracle.data_length
        return f"char({length})" if length else "char"
    if odt == "NUMBER":
        precision, scale = oracle.numeric_precision, oracle.numeric_scale
        if precision is None:
            return "numeric"
        if scale in (None, 0):
            if precision <= 4:
                return "smallint"
            if precision <= 9:
                return "integer"
            if precision <= 18:
                return "bigint"
            return f"numeric({precision},0)"
        return f"numeric({precision},{scale})"
    if odt == "DATE" or odt.startswith("TIMESTAMP"):
        return "timestamp"
    if odt.startswith("INTERVAL"):
        return "interval"
    if odt == "BOOLEAN":
        return "boolean"
    if odt in {"FLOAT", "BINARY_FLOAT"}:
        return "real"
    if odt == "BINARY_DOUBLE":
        return "double precision"
    if odt in {"RAW", "BLOB", "LONG RAW"}:
        return "bytea"
    if "CLOB" in odt or odt == "LONG":
        return "text"
    if odt in {"ROWID", "UROWID"}:
        return "text"
    if odt == "JSON":
        return "jsonb"
    if odt == "XMLTYPE":
        return "text"
    return "text"


def _extract_length(type_label: str) -> int | None:
    match = re.search(r"\((\d+)\)", type_label)
    return int(match.group(1)) if match else None


def _extract_precision_scale(type_label: str) -> tuple[int | None, int | None]:
    match = re.search(r"\((\d+)(?:,(\d+))?\)", type_label)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2) or 0)
