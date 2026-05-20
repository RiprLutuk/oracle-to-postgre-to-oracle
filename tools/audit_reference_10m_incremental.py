#!/usr/bin/env python3
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from psycopg import sql

from oracle_pg_sync.config import load_config
from oracle_pg_sync.db import oracle, postgres


TABLES = [
    "brim_zf_avs_ctype",
    "brim_zf_brand",
    "brim_zf_bras",
    "brim_zf_construct_ratio",
    "brim_zf_device",
    "brim_zf_device_odn_type",
    "brim_zf_device_site_type",
    "brim_zf_fdt",
    "brim_zf_fdt_area",
    "brim_zf_network_device_grp",
    "brim_zf_network_port_type",
    "brim_zf_network_batch",
    "brim_zf_network_batch_detail",
    "brim_zf_vendor",
]

INCREMENTAL_NAME_HINTS = (
    "update",
    "updated",
    "last",
    "modified",
    "create",
    "created",
    "date",
    "time",
    "timestamp",
    "rec_id",
    "batch_id",
    "seq",
)

KEY_NAME_HINTS = (
    "id",
    "code",
    "name",
    "seq",
    "no",
    "number",
    "batch",
    "rec",
    "device",
    "network",
    "vendor",
    "type",
    "fdt",
    "bras",
)


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool


def pg_ident(schema: str, table: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))


def oracle_columns(cur, owner: str, table: str) -> list[ColumnInfo]:
    cur.execute(
        """
        SELECT column_name, data_type, nullable
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :table_name
        ORDER BY column_id
        """,
        {"owner": owner.upper(), "table_name": table.upper()},
    )
    return [ColumnInfo(str(r[0]).lower(), str(r[1]), str(r[2]) == "Y") for r in cur.fetchall()]


def pg_columns(cur, schema: str, table: str) -> list[ColumnInfo]:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [ColumnInfo(str(r[0]).lower(), str(r[1]), str(r[2]) == "YES") for r in cur.fetchall()]


def oracle_count(cur, owner: str, table: str) -> int:
    cur.execute(f'SELECT COUNT(1) FROM "{owner.upper()}"."{table.upper()}"')
    return int(cur.fetchone()[0])


def pg_count(cur, schema: str, table: str) -> int:
    cur.execute(sql.SQL("SELECT COUNT(1) FROM {}").format(pg_ident(schema, table)))
    return int(cur.fetchone()[0])


def oracle_unique_constraints(cur, owner: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT acc.constraint_name, LISTAGG(LOWER(acc.column_name), ',') WITHIN GROUP (ORDER BY acc.position) cols
        FROM all_constraints ac
        JOIN all_cons_columns acc
          ON acc.owner = ac.owner
         AND acc.constraint_name = ac.constraint_name
         AND acc.table_name = ac.table_name
        WHERE ac.owner = :owner
          AND ac.table_name = :table_name
          AND ac.constraint_type IN ('P', 'U')
        GROUP BY acc.constraint_name
        ORDER BY acc.constraint_name
        """,
        {"owner": owner.upper(), "table_name": table.upper()},
    )
    return [str(r[1]) for r in cur.fetchall()]


def pg_unique_constraints(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT string_agg(a.attname, ',' ORDER BY x.ord)
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(c.conkey) WITH ORDINALITY AS x(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
        WHERE n.nspname = %s
          AND t.relname = %s
          AND c.contype IN ('p', 'u')
        GROUP BY c.oid
        ORDER BY c.conname
        """,
        (schema, table),
    )
    return [str(r[0]) for r in cur.fetchall() if r[0]]


def pg_unique_indexes(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT string_agg(a.attname, ',' ORDER BY x.ord)
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord) ON x.attnum > 0
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
        WHERE n.nspname = %s
          AND t.relname = %s
          AND i.indisunique
          AND i.indisvalid
        GROUP BY i.indexrelid
        ORDER BY 1
        """,
        (schema, table),
    )
    return [str(r[0]) for r in cur.fetchall() if r[0]]


def candidate_columns(columns: list[ColumnInfo], hints: tuple[str, ...]) -> list[str]:
    names = []
    for col in columns:
        lower = col.name.lower()
        if any(hint in lower for hint in hints):
            names.append(col.name)
    return names[:12]


def oracle_column_stats(cur, owner: str, table: str, column: str, count: int) -> dict[str, object]:
    cur.execute(
        f'''
        SELECT
          COUNT("{column.upper()}"),
          COUNT(DISTINCT "{column.upper()}"),
          MIN("{column.upper()}"),
          MAX("{column.upper()}")
        FROM "{owner.upper()}"."{table.upper()}"
        '''
    )
    non_null, distinct_count, min_value, max_value = cur.fetchone()
    distinct_count = int(distinct_count or 0)
    return {
        "non_null": int(non_null or 0),
        "nulls": count - int(non_null or 0),
        "distinct": distinct_count,
        "is_unique": count > 0 and distinct_count == count,
        "min": "" if min_value is None else str(min_value),
        "max": "" if max_value is None else str(max_value),
    }


def pg_column_stats(cur, schema: str, table: str, column: str, count: int) -> dict[str, object]:
    query = sql.SQL("SELECT COUNT({c}), COUNT(DISTINCT {c}), MIN({c}), MAX({c}) FROM {t}").format(
        c=sql.Identifier(column),
        t=pg_ident(schema, table),
    )
    cur.execute(query)
    non_null, distinct_count, min_value, max_value = cur.fetchone()
    distinct_count = int(distinct_count or 0)
    return {
        "non_null": int(non_null or 0),
        "nulls": count - int(non_null or 0),
        "distinct": distinct_count,
        "is_unique": count > 0 and distinct_count == count,
        "min": "" if min_value is None else str(min_value),
        "max": "" if max_value is None else str(max_value),
    }


def recommend(row: dict[str, object]) -> tuple[str, str]:
    if row["oracle_rowcount"] != row["pg_rowcount"]:
        return "truncate", "rowcount belum match, jangan incremental dulu"
    if row["best_key"] and row["best_incremental_column"]:
        if str(row["best_incremental_column"]) in {"batch_id", "rec_id"}:
            return "incremental_candidate", "numeric append key terlihat unik"
        return "incremental_candidate", "punya key unik dan kolom waktu/kandidat incremental"
    if row["oracle_rowcount"] and int(row["oracle_rowcount"]) <= 50000:
        return "truncate", "kecil/lookup, truncate insert lebih aman untuk delete/update lama"
    return "truncate", "belum ada key + watermark aman"


def main() -> int:
    cfg = load_config("config.yaml")
    out_dir = Path("reports") / f"reference_10m_incremental_audit_{datetime.now():%Y%m%d_%H%M%S}"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    with oracle.connect(cfg.oracle) as ora_conn, postgres.connect(cfg.postgres) as pg_conn:
        ora_cur = ora_conn.cursor()
        pg_cur = pg_conn.cursor()
        for table in TABLES:
            ora_cols = oracle_columns(ora_cur, cfg.oracle.schema, table)
            pg_cols = pg_columns(pg_cur, cfg.postgres.schema, table)
            ora_col_names = {c.name for c in ora_cols}
            pg_col_names = {c.name for c in pg_cols}
            common_cols = [c for c in ora_cols if c.name in pg_col_names]

            row: dict[str, object] = {
                "table": f"public.{table}",
                "oracle_rowcount": oracle_count(ora_cur, cfg.oracle.schema, table),
                "pg_rowcount": pg_count(pg_cur, cfg.postgres.schema, table),
                "oracle_columns": len(ora_cols),
                "pg_columns": len(pg_cols),
                "structure_match": ora_col_names == pg_col_names,
                "oracle_unique_constraints": ";".join(oracle_unique_constraints(ora_cur, cfg.oracle.schema, table)),
                "pg_unique_constraints": ";".join(pg_unique_constraints(pg_cur, cfg.postgres.schema, table)),
                "pg_unique_indexes": ";".join(pg_unique_indexes(pg_cur, cfg.postgres.schema, table)),
            }

            count = int(row["oracle_rowcount"])
            key_candidates = []
            key_stats = []
            for col in candidate_columns(common_cols, KEY_NAME_HINTS):
                stats = oracle_column_stats(ora_cur, cfg.oracle.schema, table, col, count)
                pg_stats = pg_column_stats(pg_cur, cfg.postgres.schema, table, col, int(row["pg_rowcount"]))
                key_stats.append(
                    f"{col}:ora_distinct={stats['distinct']},ora_nulls={stats['nulls']},ora_unique={stats['is_unique']},"
                    f"pg_distinct={pg_stats['distinct']},pg_nulls={pg_stats['nulls']},pg_unique={pg_stats['is_unique']}"
                )
                if stats["is_unique"] and pg_stats["is_unique"] and stats["nulls"] == 0 and pg_stats["nulls"] == 0:
                    key_candidates.append(col)

            incremental_candidates = []
            incremental_stats = []
            for col in candidate_columns(common_cols, INCREMENTAL_NAME_HINTS):
                stats = oracle_column_stats(ora_cur, cfg.oracle.schema, table, col, count)
                incremental_stats.append(
                    f"{col}:non_null={stats['non_null']},nulls={stats['nulls']},distinct={stats['distinct']},min={stats['min']},max={stats['max']}"
                )
                if stats["non_null"] and (col in key_candidates or any(h in col for h in ("update", "last", "date", "time", "rec_id", "batch_id"))):
                    incremental_candidates.append(col)

            best_key = ""
            for preferred in ("rec_id", "batch_id", "id"):
                if preferred in key_candidates:
                    best_key = preferred
                    break
            if not best_key and key_candidates:
                best_key = key_candidates[0]

            best_incremental = ""
            for preferred in ("updated_date", "last_update", "update_date", "created_date", "rec_id", "batch_id"):
                if preferred in incremental_candidates:
                    best_incremental = preferred
                    break
            if not best_incremental and incremental_candidates:
                best_incremental = incremental_candidates[0]

            row["key_candidates"] = ";".join(key_candidates)
            row["incremental_candidates"] = ";".join(incremental_candidates)
            row["best_key"] = best_key
            row["best_incremental_column"] = best_incremental
            row["key_stats"] = " | ".join(key_stats)
            row["incremental_stats"] = " | ".join(incremental_stats)
            row["mode_recommendation"], row["reason"] = recommend(row)
            rows.append(row)

    csv_path = out_dir / "reference_10m_incremental_audit.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    md_path = out_dir / "reference_10m_incremental_audit.md"
    lines = [
        "# Audit Incremental Reference 10 Menit",
        "",
        f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        "",
        "| Table | Ora Rows | PG Rows | Struktur | Best Key | Watermark | Rekomendasi | Catatan |",
        "|---|---:|---:|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {table} | {oracle_rowcount} | {pg_rowcount} | {structure_match} | {best_key} | {best_incremental_column} | {mode_recommendation} | {reason} |".format(
                **row
            )
        )
    lines.extend(["", f"CSV detail: `{csv_path}`", ""])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
