from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from oracle_pg_sync.config import AppConfig, load_config
from oracle_pg_sync.utils.logging import setup_logging
from oracle_pg_sync.utils.naming import split_schema_table


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="oracle-pg-sync-audit")
    parser.add_argument("--config", default="config.yaml", help="Path config YAML/JSON")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    sub = parser.add_subparsers(dest="command", required=True)

    audit = sub.add_parser("audit", help="Cek metadata, rowcount, dependency")
    _add_common_args(audit)
    audit.add_argument("--tables", nargs="*", help="Override table list")
    audit.add_argument("--tables-file", help="Read table list from YAML/JSON file")
    audit.add_argument(
        "--all-postgres-tables",
        action="store_true",
        help="Audit all tables discovered from PostgreSQL schema, ignoring config table list",
    )
    audit.add_argument("--limit", type=int, help="Limit table count after table selection")
    audit.add_argument("--fast-count", action="store_true", help="Use statistic count")
    audit.add_argument("--exact-count", action="store_true", help="Use SELECT COUNT(1)")
    audit.add_argument("--workers", type=int, default=1, help="Parallel audit workers. Default 1 agar ringan")
    audit.add_argument("--suggest-drop", action="store_true", help="Include DROP COLUMN suggestions for PG-only columns")
    audit.add_argument("--sql-out", help="Path output SQL suggestion. Default: reports/schema_suggestions.sql")

    sync = sub.add_parser("sync", help="Sync data antar Oracle dan PostgreSQL")
    _add_common_args(sync)
    sync.add_argument("--tables", nargs="*", help="Override table list")
    sync.add_argument("--tables-file", help="Read table list from YAML/JSON file")
    sync.add_argument("--limit", type=int, help="Limit table count after table selection")
    sync.add_argument(
        "--direction",
        choices=["oracle-to-postgres", "postgres-to-oracle"],
        help="Arah sync. Default dari sync.default_direction.",
    )
    sync.add_argument("--mode", choices=["truncate", "swap", "append", "upsert", "delete"], help="Override mode")
    sync.add_argument("--execute", action="store_true", help="Benar-benar eksekusi perubahan data")
    sync.add_argument("--force", action="store_true", help="Tetap sync walaupun struktur mismatch")

    report = sub.add_parser("report", help="Generate report.html dari file CSV reports")
    _add_common_args(report)
    report.add_argument("--tables", nargs="*", help="Tidak dipakai, disediakan agar konsisten")

    objects = sub.add_parser("audit-objects", help="Compare schema objects seperti view, sequence, SP/function, trigger")
    _add_common_args(objects)
    objects.add_argument(
        "--types",
        nargs="*",
        help="Object types. Default: view, materialized view, sequence, procedure, function, package, trigger, synonym",
    )
    objects.add_argument(
        "--include-extension-objects",
        action="store_true",
        help="Include PostgreSQL extension-owned objects such as pg_trgm or pg_stat_statements",
    )

    all_cmd = sub.add_parser("all", help="Audit, sync, audit ulang, lalu report")
    _add_common_args(all_cmd)
    all_cmd.add_argument("--tables", nargs="*", help="Override table list")
    all_cmd.add_argument("--tables-file", help="Read table list from YAML/JSON file")
    all_cmd.add_argument("--limit", type=int, help="Limit table count after table selection")
    all_cmd.add_argument(
        "--direction",
        choices=["oracle-to-postgres", "postgres-to-oracle"],
        help="Arah sync. Default dari sync.default_direction.",
    )
    all_cmd.add_argument("--mode", choices=["truncate", "swap", "append", "upsert", "delete"], help="Override mode")
    all_cmd.add_argument("--execute", action="store_true", help="Benar-benar eksekusi perubahan data")
    all_cmd.add_argument("--force", action="store_true", help="Tetap sync walaupun struktur mismatch")
    all_cmd.add_argument("--fast-count", action="store_true", help="Use statistic count")
    all_cmd.add_argument("--exact-count", action="store_true", help="Use SELECT COUNT(1)")
    all_cmd.add_argument("--workers", type=int, default=1, help="Parallel audit workers. Default 1 agar ringan")
    all_cmd.add_argument("--suggest-drop", action="store_true", help="Include DROP COLUMN suggestions for PG-only columns")
    all_cmd.add_argument("--sql-out", help="Path output SQL suggestion. Default: reports/schema_suggestions.sql")

    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default=argparse.SUPPRESS, help="Path config YAML/JSON")
    parser.add_argument("--verbose", action="store_true", default=argparse.SUPPRESS, help="Enable debug logging")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    if args.command in {"audit", "sync", "all", "audit-objects"}:
        _ensure_oracle_client_library_path(config, argv)
    report_dir = Path(config.reports.output_dir)
    logger = setup_logging(report_dir, logging.DEBUG if args.verbose else logging.INFO)
    direction = _resolve_direction(config, getattr(args, "direction", None)) if args.command in {"sync", "all"} else None
    tables = _resolve_tables(
        config,
        getattr(args, "tables", None),
        direction=direction,
        tables_file=getattr(args, "tables_file", None),
        limit=getattr(args, "limit", None),
    )

    if getattr(args, "exact_count", False):
        config.sync.fast_count = False
        logger.warning("Exact count memakai SELECT COUNT(1); untuk tabel besar ini bisa berat.")
    if getattr(args, "fast_count", False):
        config.sync.fast_count = True

    if args.command == "audit" and args.all_postgres_tables:
        tables = _apply_limit(_discover_postgres_tables(config, logger), getattr(args, "limit", None))
    elif not tables and args.command == "audit":
        tables = _discover_postgres_tables(config, logger)

    if not tables and args.command != "report":
        raise SystemExit("Tidak ada table target. Isi config.tables atau pakai --tables.")

    if args.command == "audit":
        from oracle_pg_sync.reports import write_audit_reports

        audit_result = run_audit(config, tables, logger, workers=args.workers)
        write_audit_reports(
            report_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
            sql_suggestions_path=_sql_suggestions_path(report_dir, getattr(args, "sql_out", None)),
            suggest_drop=args.suggest_drop,
        )
        logger.info("Audit selesai. Report ada di %s", report_dir)
        return 0

    if args.command == "sync":
        from oracle_pg_sync.reports.writer_csv import write_csv

        results = _sync_runner(config, logger, direction).sync_tables(
            tables,
            mode_override=args.mode,
            execute=args.execute,
            force=args.force,
        )
        rows = [result.as_row() for result in results]
        write_csv(report_dir / "sync_result.csv", rows)
        logger.info("Sync selesai. SUCCESS=%s FAILED=%s SKIPPED=%s DRY_RUN=%s",
                    _count(rows, "SUCCESS"), _count(rows, "FAILED"), _count(rows, "SKIPPED"), _count(rows, "DRY_RUN"))
        return 1 if any(row["status"] == "FAILED" for row in rows) else 0

    if args.command == "report":
        from oracle_pg_sync.reports.writer_html import write_html_report

        inventory_rows = _read_csv(report_dir / "inventory_summary.csv")
        column_diff_rows = _read_csv(report_dir / "column_diff.csv")
        sync_rows = _read_csv(report_dir / "sync_result.csv")
        write_html_report(
            report_dir / "report.html",
            inventory_rows=inventory_rows,
            column_diff_rows=column_diff_rows,
            sync_rows=sync_rows,
        )
        logger.info("HTML report dibuat: %s", report_dir / "report.html")
        return 0

    if args.command == "audit-objects":
        from oracle_pg_sync.reports.writer_csv import write_csv

        result = run_object_audit(
            config,
            logger,
            object_types=getattr(args, "types", None),
            include_extension_objects=args.include_extension_objects,
        )
        write_csv(report_dir / "object_inventory.csv", result.inventory_rows)
        write_csv(report_dir / "object_compare.csv", result.compare_rows)
        logger.info(
            "Object audit selesai. MATCH=%s MISSING_IN_ORACLE=%s MISSING_IN_POSTGRES=%s",
            _count(result.compare_rows, "MATCH"),
            _count(result.compare_rows, "MISSING_IN_ORACLE"),
            _count(result.compare_rows, "MISSING_IN_POSTGRES"),
        )
        return 0

    if args.command == "all":
        from oracle_pg_sync.reports import write_audit_reports
        from oracle_pg_sync.reports.writer_csv import write_csv

        logger.info("Step 1/3 audit awal")
        run_audit(config, tables, logger, workers=args.workers)
        logger.info("Step 2/3 sync direction=%s", direction)
        sync_rows = [
            result.as_row()
            for result in _sync_runner(config, logger, direction).sync_tables(
                tables,
                mode_override=args.mode,
                execute=args.execute,
                force=args.force,
            )
        ]
        write_csv(report_dir / "sync_result.csv", sync_rows)
        logger.info("Step 3/3 audit ulang dan report")
        audit_result = run_audit(config, tables, logger, workers=args.workers)
        write_audit_reports(
            report_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
            sql_suggestions_path=_sql_suggestions_path(report_dir, getattr(args, "sql_out", None)),
            suggest_drop=args.suggest_drop,
            sync_rows=sync_rows,
        )
        return 1 if any(row["status"] == "FAILED" for row in sync_rows) else 0

    return 2


def run_audit(config: AppConfig, tables: list[str], logger: logging.Logger, *, workers: int = 1):
    from oracle_pg_sync.db import oracle, postgres
    from oracle_pg_sync.metadata.compare import AuditResult

    inventory_rows: list[dict] = []
    column_diff_rows: list[dict] = []
    type_mismatch_rows: list[dict] = []
    dependency_rows: list[dict] = []

    worker_count = max(1, int(workers or 1))
    if worker_count > 1:
        logger.info("Audit parallel workers=%s", worker_count)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(_audit_table_with_new_connections, config, table_name, logger): table_name
                for table_name in tables
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    table = split_schema_table(futures[future], config.postgres.schema)
                    logger.exception("Audit failed for %s", table.fqname)
                    result = (
                        {
                            "table_name": table.fqname,
                            "oracle_exists": "",
                            "postgres_exists": "",
                            "status": "MISMATCH",
                            "error": str(exc),
                        },
                        [],
                        [],
                        [],
                    )
                _merge_audit_result(result, inventory_rows, column_diff_rows, type_mismatch_rows, dependency_rows)
    else:
        with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
            with ocon.cursor() as ocur, pcon.cursor() as pcur:
                for table_name in tables:
                    _merge_audit_result(
                        _audit_table(config, table_name, ocur, pcur, logger),
                        inventory_rows,
                        column_diff_rows,
                        type_mismatch_rows,
                        dependency_rows,
                    )

    return AuditResult(inventory_rows, column_diff_rows, type_mismatch_rows, dependency_rows)


def run_object_audit(
    config: AppConfig,
    logger: logging.Logger,
    *,
    object_types: list[str] | None = None,
    include_extension_objects: bool = False,
):
    from oracle_pg_sync.db import oracle, postgres
    from oracle_pg_sync.metadata.object_compare import ObjectAuditResult, compare_object_inventory, normalize_object_types

    types = normalize_object_types(object_types)
    logger.info("Audit schema objects types=%s", ",".join(sorted(types)))
    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            oracle_rows = oracle.schema_object_rows(ocur, config.oracle.schema, types)
            postgres_rows = postgres.schema_object_rows(
                pcur,
                config.postgres.schema,
                types,
                include_extension_objects=include_extension_objects,
            )
    inventory_rows = oracle_rows + postgres_rows
    compare_rows = compare_object_inventory(oracle_rows, postgres_rows)
    return ObjectAuditResult(inventory_rows, compare_rows)


def _audit_table_with_new_connections(
    config: AppConfig,
    table_name: str,
    logger: logging.Logger,
) -> tuple[dict, list[dict], list[dict], list[dict]]:
    from oracle_pg_sync.db import oracle, postgres

    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            return _audit_table(config, table_name, ocur, pcur, logger)


def _audit_table(config: AppConfig, table_name: str, ocur, pcur, logger: logging.Logger) -> tuple[dict, list[dict], list[dict], list[dict]]:
    from oracle_pg_sync.db import oracle, postgres
    from oracle_pg_sync.metadata.compare import compare_table_metadata
    from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
    from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata

    owner = config.oracle.schema
    table = split_schema_table(table_name, config.postgres.schema)
    logger.info("Audit %s", table.fqname)
    try:
        oracle_meta = fetch_oracle_metadata(
            ocur,
            owner=owner,
            table=table.table,
            fast_count=config.sync.fast_count,
        )
        pg_meta = fetch_pg_metadata(
            pcur,
            schema=table.schema,
            table=table.table,
            fast_count=config.sync.fast_count,
        )
        inventory, diffs, mismatches = compare_table_metadata(
            table_name=table.fqname,
            config=config,
            oracle_meta=oracle_meta,
            postgres_meta=pg_meta,
        )
        dependencies = oracle.dependency_rows(ocur, owner, [table.table])
        dependencies.extend(postgres.dependency_rows(pcur, table.schema, table.table))
        return inventory, diffs, mismatches, dependencies
    except Exception as exc:
        logger.exception("Audit failed for %s", table.fqname)
        return (
            {
                "table_name": table.fqname,
                "oracle_exists": "",
                "postgres_exists": "",
                "status": "MISMATCH",
                "error": str(exc),
            },
            [],
            [],
            [],
        )


def _merge_audit_result(
    result: tuple[dict, list[dict], list[dict], list[dict]],
    inventory_rows: list[dict],
    column_diff_rows: list[dict],
    type_mismatch_rows: list[dict],
    dependency_rows: list[dict],
) -> None:
    inventory, diffs, mismatches, dependencies = result
    inventory_rows.append(inventory)
    column_diff_rows.extend(diffs)
    type_mismatch_rows.extend(mismatches)
    dependency_rows.extend(dependencies)


def _resolve_tables(
    config: AppConfig,
    override: list[str] | None,
    *,
    direction: str | None = None,
    tables_file: str | None = None,
    limit: int | None = None,
) -> list[str]:
    if limit is not None and limit < 1:
        raise SystemExit("--limit must be greater than 0")
    if override:
        return _apply_limit(override, limit)
    if tables_file:
        return _apply_limit(_read_table_names_file(Path(tables_file), direction=direction), limit)
    if direction:
        return _apply_limit(config.table_names_for_direction(direction), limit)
    return _apply_limit(config.table_names(), limit)


def _apply_limit(tables: list[str], limit: int | None) -> list[str]:
    return tables[:limit] if limit is not None else tables


def _read_table_names_file(path: Path, *, direction: str | None = None) -> list[str]:
    if not path.exists():
        raise SystemExit(f"Tables file not found: {path}")
    raw = _read_structured_file(path)
    rows = raw.get("tables") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        raise SystemExit(f"Tables file must contain a list or a 'tables' list: {path}")
    tables: list[str] = []
    for row in rows:
        if isinstance(row, str):
            tables.append(row)
            continue
        if not isinstance(row, dict):
            continue
        directions = [str(item).lower() for item in row.get("directions", [])]
        if direction and directions and direction not in directions:
            continue
        name = str(row.get("name") or "").strip()
        if name:
            tables.append(name)
    return tables


def _read_structured_file(path: Path):
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        import json

        return json.loads(text)
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise SystemExit("PyYAML belum terinstall. Jalankan: pip install -r requirements.txt") from exc
    return yaml.safe_load(text) or {}


def _ensure_oracle_client_library_path(config: AppConfig, argv: list[str] | None) -> None:
    lib_dir = config.oracle.client_lib_dir
    if not lib_dir or os.name == "nt" or os.environ.get("ORACLE_PG_SYNC_REEXEC") == "1":
        return
    lib_path = str(Path(lib_dir).expanduser())
    current = os.environ.get("LD_LIBRARY_PATH", "")
    paths = [item for item in current.split(":") if item]
    if lib_path in paths:
        return
    if not Path(lib_path).exists():
        return
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = lib_path if not current else f"{lib_path}:{current}"
    env["ORACLE_PG_SYNC_REEXEC"] = "1"
    script_args = [sys.executable, "-m", "oracle_pg_sync", *(sys.argv[1:] if argv is None else argv)]
    os.execvpe(sys.executable, script_args, env)


def _discover_postgres_tables(config: AppConfig, logger: logging.Logger) -> list[str]:
    from oracle_pg_sync.db import postgres

    logger.info("Tidak ada table list. Ambil semua table dari PostgreSQL schema=%s", config.postgres.schema)
    with postgres.connect(config.postgres, autocommit=True) as pcon:
        with pcon.cursor() as cur:
            tables = postgres.list_tables(cur, config.postgres.schema)
    logger.info("Ditemukan %s table dari PostgreSQL", len(tables))
    return tables


def _sql_suggestions_path(report_dir: Path, override: str | None) -> Path:
    return Path(override) if override else report_dir / "schema_suggestions.sql"


def _resolve_direction(config: AppConfig, override: str | None) -> str:
    direction = (override or config.sync.default_direction or "oracle-to-postgres").lower()
    if direction not in {"oracle-to-postgres", "postgres-to-oracle"}:
        raise SystemExit(f"Unsupported sync direction: {direction}")
    return direction


def _sync_runner(config: AppConfig, logger: logging.Logger, direction: str):
    if direction == "postgres-to-oracle":
        from oracle_pg_sync.sync.postgres_to_oracle import PostgresToOracleSync

        return PostgresToOracleSync(config, logger)
    from oracle_pg_sync.sync.oracle_to_postgres import OracleToPostgresSync

    return OracleToPostgresSync(config, logger)


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _count(rows: list[dict], status: str) -> int:
    return sum(1 for row in rows if row.get("status") == status)


if __name__ == "__main__":
    raise SystemExit(main())
