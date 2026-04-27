from __future__ import annotations

import argparse
import csv
import logging
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
    audit.add_argument("--fast-count", action="store_true", help="Use statistic count")
    audit.add_argument("--exact-count", action="store_true", help="Use SELECT COUNT(1)")

    sync = sub.add_parser("sync", help="Sync data antar Oracle dan PostgreSQL")
    _add_common_args(sync)
    sync.add_argument("--tables", nargs="*", help="Override table list")
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

    all_cmd = sub.add_parser("all", help="Audit, sync, audit ulang, lalu report")
    _add_common_args(all_cmd)
    all_cmd.add_argument("--tables", nargs="*", help="Override table list")
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

    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default=argparse.SUPPRESS, help="Path config YAML/JSON")
    parser.add_argument("--verbose", action="store_true", default=argparse.SUPPRESS, help="Enable debug logging")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    report_dir = Path(config.reports.output_dir)
    logger = setup_logging(report_dir, logging.DEBUG if args.verbose else logging.INFO)
    direction = _resolve_direction(config, getattr(args, "direction", None)) if args.command in {"sync", "all"} else None
    tables = _resolve_tables(config, getattr(args, "tables", None), direction=direction)

    if getattr(args, "exact_count", False):
        config.sync.fast_count = False
        logger.warning("Exact count memakai SELECT COUNT(1); untuk tabel besar ini bisa berat.")
    if getattr(args, "fast_count", False):
        config.sync.fast_count = True

    if not tables and args.command != "report":
        raise SystemExit("Tidak ada table target. Isi config.tables atau pakai --tables.")

    if args.command == "audit":
        from oracle_pg_sync.reports import write_audit_reports

        audit_result = run_audit(config, tables, logger)
        write_audit_reports(
            report_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
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

    if args.command == "all":
        from oracle_pg_sync.reports import write_audit_reports
        from oracle_pg_sync.reports.writer_csv import write_csv

        logger.info("Step 1/3 audit awal")
        run_audit(config, tables, logger)
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
        audit_result = run_audit(config, tables, logger)
        write_audit_reports(
            report_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
            sync_rows=sync_rows,
        )
        return 1 if any(row["status"] == "FAILED" for row in sync_rows) else 0

    return 2


def run_audit(config: AppConfig, tables: list[str], logger: logging.Logger):
    from oracle_pg_sync.db import oracle, postgres
    from oracle_pg_sync.metadata.compare import AuditResult, compare_table_metadata
    from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
    from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata

    inventory_rows: list[dict] = []
    column_diff_rows: list[dict] = []
    type_mismatch_rows: list[dict] = []
    dependency_rows: list[dict] = []
    owner = config.oracle.schema

    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            for table_name in tables:
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
                    inventory_rows.append(inventory)
                    column_diff_rows.extend(diffs)
                    type_mismatch_rows.extend(mismatches)
                    dependency_rows.extend(oracle.dependency_rows(ocur, owner, [table.table]))
                    dependency_rows.extend(postgres.dependency_rows(pcur, table.schema, table.table))
                except Exception as exc:
                    logger.exception("Audit failed for %s", table.fqname)
                    inventory_rows.append(
                        {
                            "table_name": table.fqname,
                            "oracle_exists": "",
                            "postgres_exists": "",
                            "status": "MISMATCH",
                            "error": str(exc),
                        }
                    )

    return AuditResult(inventory_rows, column_diff_rows, type_mismatch_rows, dependency_rows)


def _resolve_tables(config: AppConfig, override: list[str] | None, *, direction: str | None = None) -> list[str]:
    if override:
        return override
    if direction:
        return config.table_names_for_direction(direction)
    return config.table_names()


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
