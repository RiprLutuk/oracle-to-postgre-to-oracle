from __future__ import annotations

import argparse
import atexit
import csv
import fcntl
import json
import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from oracle_pg_sync.alerting import send_alert
from oracle_pg_sync.checkpoint import CheckpointStore, new_run_id
from oracle_pg_sync.config import AppConfig, TableConfig, load_config
from oracle_pg_sync.dependency_health import critical_dependency_rows, summarize_dependency_rows
from oracle_pg_sync.manifest import RunManifest
from oracle_pg_sync.manifest import sanitize
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
    audit.add_argument("--sql-out", help="Path output SQL suggestion. Default: current run dir/schema_suggestions.sql")

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
    sync.add_argument(
        "--mode",
        choices=["truncate", "swap", "append", "upsert", "delete", "truncate_safe", "swap_safe", "incremental_safe"],
        help="Override mode",
    )
    sync.add_argument(
        "--where",
        help="Override WHERE filter for this sync run. Intended for one-table jobs, for example cron upsert windows.",
    )
    sync.add_argument("--key-columns", nargs="+", help="Override key columns for one-table upsert jobs")
    _add_incremental_override_args(sync)
    sync.add_argument("--execute", "--go", dest="execute", action="store_true", help="Benar-benar eksekusi perubahan data")
    sync.add_argument("--lob", choices=["error", "skip", "null", "stream", "include"], help="Override default LOB strategy")
    sync.add_argument("--force", action="store_true", help="Tetap sync walaupun struktur mismatch")
    sync.add_argument("--simulate", action="store_true", help="Risk simulation only; no data changes")
    _add_production_sync_args(sync)

    report = sub.add_parser("report", help="Generate report.html dari CSV latest run")
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

    dependencies = sub.add_parser("dependencies", help="List view/SP/function/trigger/sequence dependencies per table")
    _add_common_args(dependencies)
    dependencies.add_argument("--tables", nargs="*", help="Override table list")
    dependencies.add_argument("--tables-file", help="Read table list from YAML/JSON file")
    dependencies.add_argument("--limit", type=int, help="Limit table count after table selection")
    dependencies.add_argument(
        "--out",
        help="Path output CSV. Default: reports/table_object_dependencies.csv",
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
    all_cmd.add_argument(
        "--mode",
        choices=["truncate", "swap", "append", "upsert", "delete", "truncate_safe", "swap_safe", "incremental_safe"],
        help="Override mode",
    )
    all_cmd.add_argument(
        "--where",
        help="Override WHERE filter for the sync step. Intended for one-table jobs.",
    )
    all_cmd.add_argument("--key-columns", nargs="+", help="Override key columns for one-table upsert jobs")
    _add_incremental_override_args(all_cmd)
    all_cmd.add_argument("--execute", "--go", dest="execute", action="store_true", help="Benar-benar eksekusi perubahan data")
    all_cmd.add_argument("--lob", choices=["error", "skip", "null", "stream", "include"], help="Override default LOB strategy")
    all_cmd.add_argument("--force", action="store_true", help="Tetap sync walaupun struktur mismatch")
    all_cmd.add_argument("--simulate", action="store_true", help="Risk simulation only; no data changes")
    _add_production_sync_args(all_cmd)
    all_cmd.add_argument("--fast-count", action="store_true", help="Use statistic count")
    all_cmd.add_argument("--exact-count", action="store_true", help="Use SELECT COUNT(1)")
    all_cmd.add_argument("--workers", type=int, default=1, help="Parallel audit workers. Default 1 agar ringan")
    all_cmd.add_argument("--suggest-drop", action="store_true", help="Include DROP COLUMN suggestions for PG-only columns")
    all_cmd.add_argument("--sql-out", help="Path output SQL suggestion. Default: current run dir/schema_suggestions.sql")

    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default=argparse.SUPPRESS, help="Path config YAML/JSON")
    parser.add_argument("--verbose", action="store_true", default=argparse.SUPPRESS, help="Enable debug logging")


def _add_production_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", choices=["daily", "every_5min"], help="Apply DBA job defaults for daily or every_5min runs")
    parser.add_argument("--resume", metavar="RUN_ID", help="Resume sync run from checkpoint")
    parser.add_argument("--reset-checkpoint", metavar="RUN_ID", help="Delete checkpoint state for RUN_ID and exit")
    parser.add_argument("--list-runs", action="store_true", help="List checkpoint runs and exit")
    parser.add_argument("--incremental", action="store_true", help="Use table incremental config and stored watermarks")
    parser.add_argument("--full-refresh", action="store_true", help="Ignore incremental watermark filter for this run")
    parser.add_argument("--watermark-status", action="store_true", help="List stored watermarks and exit")
    parser.add_argument("--reset-watermark", metavar="TABLE_NAME", help="Delete stored watermark for TABLE_NAME and exit")
    parser.add_argument("--lock-file", default="reports/sync.lock", help="Lock file path for scheduled jobs")
    parser.add_argument("--no-lock", action="store_true", help="Disable lock file protection")
    parser.add_argument("--log-rotate-bytes", type=int, default=10 * 1024 * 1024, help="Rotate reports/sync.log above this size")


def _add_incremental_override_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--incremental-column", help="Enable incremental override using this source column")
    parser.add_argument(
        "--incremental-strategy",
        choices=["updated_at", "numeric_key"],
        default=argparse.SUPPRESS,
        help="Incremental override strategy. Default: updated_at",
    )
    parser.add_argument("--initial-value", help="Initial watermark value when no stored watermark exists")
    parser.add_argument("--overlap-minutes", type=int, default=argparse.SUPPRESS, help="Updated-at overlap minutes")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _apply_profile(args)
    config = load_config(args.config)
    if args.command in {"audit", "sync", "all", "audit-objects", "dependencies"}:
        _ensure_oracle_client_library_path(config, argv)
    report_dir = Path(config.reports.output_dir)
    _rotate_log(report_dir / "sync.log", max_bytes=int(getattr(args, "log_rotate_bytes", 10 * 1024 * 1024) or 0))
    logger = setup_logging(report_dir, logging.DEBUG if args.verbose else logging.INFO)
    _maybe_acquire_lock(args, logger)
    direction = _resolve_direction(config, getattr(args, "direction", None)) if args.command in {"sync", "all"} else None
    checkpoint_store = CheckpointStore(config.sync.checkpoint_dir)

    if args.command in {"sync", "all"}:
        if getattr(args, "list_runs", False):
            _print_rows(checkpoint_store.list_runs())
            return 0
        if getattr(args, "reset_checkpoint", None):
            checkpoint_store.reset_run(args.reset_checkpoint)
            logger.info("Checkpoint run dihapus: %s", args.reset_checkpoint)
            return 0
        if getattr(args, "watermark_status", False):
            _print_rows(checkpoint_store.list_watermarks())
            return 0
        if getattr(args, "reset_watermark", None):
            count = checkpoint_store.reset_watermark(args.reset_watermark)
            logger.info("Watermark dihapus untuk %s rows=%s", args.reset_watermark, count)
            return 0

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

    _apply_lob_override(config, getattr(args, "lob", None))
    if args.command == "audit" and args.all_postgres_tables:
        tables = _apply_limit(_discover_postgres_tables(config, logger), getattr(args, "limit", None))
    elif not tables and args.command == "audit":
        tables = _discover_postgres_tables(config, logger)

    if not tables and args.command not in {"report", "audit-objects"}:
        raise SystemExit("Tidak ada table target. Isi config.tables atau pakai --tables.")
    _apply_runtime_table_overrides(args, config, tables)
    job_key = _job_key(config, args, direction, tables) if args.command in {"sync", "all"} else ""

    if args.command in {"sync", "all"}:
        blocked = checkpoint_store.job_blocked(job_key, max_failures=config.sync.max_failures)
        if blocked:
            payload = _alert_payload(
                run_id="",
                direction=direction,
                error=f"circuit breaker active until {blocked.get('cooldown_until')}",
                failed_tables=tables,
            )
            send_alert(config, event="repeated_failure", payload=payload, logger=logger)
            logger.error("Circuit breaker active for %s until %s", job_key, blocked.get("cooldown_until"))
            return 1
        if getattr(args, "simulate", False):
            return _simulate_sync(config, tables, logger, direction=direction, mode=getattr(args, "mode", None))

    if args.command == "audit":
        from oracle_pg_sync.reports import write_audit_reports
        from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx

        run_id = new_run_id()
        manifest = RunManifest(
            report_dir=report_dir,
            run_id=run_id,
            command="audit",
            config_file=args.config,
            config=config,
            direction=None,
            dry_run=True,
            tables_requested=tables,
            checkpoint_path=str(checkpoint_store.path),
        )
        audit_result = run_audit(config, tables, logger, workers=args.workers)
        run_dir = manifest.run_dir
        write_audit_reports(
            run_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
            sql_suggestions_path=_sql_suggestions_path(run_dir, getattr(args, "sql_out", None)),
            suggest_drop=args.suggest_drop,
        )
        dependency_summary_rows = _write_audit_run_reports(
            manifest,
            report_dir=report_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
            config=config,
            write_central_report_xlsx=write_central_report_xlsx,
        )
        manifest_path = manifest.finish(
            result_rows=audit_result.inventory_rows,
            dependency_rows=dependency_summary_rows,
            report_files=_run_report_files(
                run_dir,
                "inventory_summary.csv",
                "inventory_summary.xlsx",
                "column_diff.csv",
                "type_mismatch.csv",
                "object_dependency_summary.csv",
                "schema_suggestions.sql",
                "report.xlsx",
                "report.html",
                "logs.txt",
            ),
        )
        logger.info("Manifest dibuat: %s", manifest_path)
        logger.info("Audit selesai. Report ada di %s", report_dir)
        return 0

    if args.command == "sync":
        from oracle_pg_sync.reports.writer_csv import write_csv
        from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx, write_rows_xlsx
        from oracle_pg_sync.reports.writer_html import write_html_report
        from oracle_pg_sync.rollback import rollback_run

        run_id = args.resume or new_run_id()
        manifest = RunManifest(
            report_dir=report_dir,
            run_id=run_id,
            command="sync",
            config_file=args.config,
            config=config,
            direction=direction,
            dry_run=not args.execute or config.sync.dry_run and not args.execute,
            tables_requested=tables,
            checkpoint_path=str(checkpoint_store.path),
        )
        run_dir = manifest.run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        dependency_pre_rows = _write_dependency_report(config, tables, logger, run_dir, phase="pre")
        results = _sync_runner(config, logger, direction).sync_tables(
            tables,
            mode_override=args.mode,
            execute=args.execute,
            force=args.force,
            checkpoint_store=checkpoint_store,
            run_id=run_id,
            resume=bool(args.resume),
            incremental=args.incremental,
            full_refresh=args.full_refresh,
        )
        rows = [result.as_row() for result in results]
        write_csv(run_dir / "sync_result.csv", rows)
        write_rows_xlsx(run_dir / "sync_result.xlsx", rows, sheet_name="sync_result")
        checksum_rows = _checksum_rows_from_results(results, rows)
        if checksum_rows:
            write_csv(run_dir / "validation_checksum.csv", checksum_rows)
            write_rows_xlsx(run_dir / "validation_checksum.xlsx", checksum_rows, sheet_name="checksum")
        maintenance_rows = _run_dependency_maintenance(
            config,
            tables,
            logger,
            run_dir,
            dependency_pre_rows,
            execute=args.execute,
        )
        dependency_post_rows = _write_dependency_report(config, tables, logger, run_dir, phase="post")
        dependency_rows = dependency_pre_rows + dependency_post_rows
        dependency_summary_rows = _write_dependency_summary(run_dir, dependency_rows, maintenance_rows)
        dependency_failed = _dependency_failed(config, dependency_rows + maintenance_rows)
        rollback_rows: list[dict] = []
        table_failed = any(row["status"] == "FAILED" for row in rows)
        run_failed = dependency_failed or table_failed
        if args.execute and dependency_failed:
            rollback_rows = rollback_run(config, checkpoint_store, run_id=run_id, logger=logger)
            write_csv(run_dir / "rollback_result.csv", rollback_rows)
        if args.execute and not run_failed:
            _apply_watermark_updates(checkpoint_store, results)
            checkpoint_store.clear_job_failures(job_key)
        elif args.execute:
            checkpoint_store.register_job_failure(
                job_key,
                cooldown_minutes=config.sync.cooldown_minutes,
                error_message=_first_error(rows, maintenance_rows, dependency_failed),
            )
            event = "dependency_error" if dependency_failed else "failure"
            send_alert(
                config,
                event=event,
                payload=_alert_payload(
                    run_id=run_id,
                    direction=direction,
                    error=_first_error(rows, maintenance_rows, dependency_failed),
                    failed_tables=[row["table_name"] for row in rows if row.get("status") == "FAILED"],
                ),
                logger=logger,
            )
        metrics_rows = _metrics_rows(results, rollback_rows)
        _write_metrics_json(run_dir, metrics_rows)
        _write_run_reports(
            manifest,
            report_dir=report_dir,
            sync_rows=rows,
            checksum_rows=checksum_rows,
            dependency_rows=dependency_rows,
            dependency_summary_rows=dependency_summary_rows,
            maintenance_rows=maintenance_rows,
            watermark_rows=checkpoint_store.list_watermarks(),
            checkpoint_rows=checkpoint_store.list_chunks(run_id),
            rollback_rows=rollback_rows,
            timeline_rows=checkpoint_store.list_events(run_id),
            config=config,
            write_central_report_xlsx=write_central_report_xlsx,
            write_html_report=write_html_report,
        )
        manifest_path = manifest.finish(
            result_rows=rows,
            checksum_rows=checksum_rows,
            lob_rows=rows,
            dependency_rows=dependency_summary_rows,
            metrics_rows=metrics_rows,
            rollback_rows=rollback_rows,
            timeline_rows=checkpoint_store.list_events(run_id),
            report_files=_run_report_files(
                run_dir,
                "sync_result.csv",
                "sync_result.xlsx",
                "validation_checksum.csv",
                "validation_checksum.xlsx",
                "metrics.json",
                "dependency_pre.csv",
                "dependency_post.csv",
                "dependency_maintenance.csv",
                "dependency_summary.csv",
                "rollback_result.csv",
                "report.xlsx",
                "report.html",
                "logs.txt",
            ),
        )
        logger.info("Manifest dibuat: %s", manifest_path)
        logger.info("Sync selesai. SUCCESS=%s FAILED=%s SKIPPED=%s DRY_RUN=%s",
                    _count(rows, "SUCCESS"), _count(rows, "FAILED"), _count(rows, "SKIPPED"), _count(rows, "DRY_RUN"))
        return 1 if run_failed else 0

    if args.command == "report":
        from oracle_pg_sync.reports.writer_html import write_html_report

        source_dir = _latest_run_dir(report_dir) or report_dir
        inventory_rows = _read_csv(source_dir / "inventory_summary.csv")
        column_diff_rows = _read_csv(source_dir / "column_diff.csv")
        sync_rows = _read_csv(source_dir / "sync_result.csv")
        checksum_rows = _read_csv(source_dir / "validation_checksum.csv")
        dependency_rows = _read_csv(source_dir / "dependency_pre.csv") + _read_csv(source_dir / "dependency_post.csv")
        if not dependency_rows:
            dependency_rows = _read_csv(source_dir / "object_dependency_summary.csv") + _read_csv(
                source_dir / "table_object_dependencies.csv"
            )
        maintenance_rows = _read_csv(source_dir / "dependency_maintenance.csv")
        dependency_summary_rows = _read_csv(source_dir / "dependency_summary.csv")
        if not dependency_summary_rows and dependency_rows:
            dependency_summary_rows = summarize_dependency_rows(dependency_rows, maintenance_rows)
        write_html_report(
            source_dir / "report.html",
            inventory_rows=inventory_rows,
            column_diff_rows=column_diff_rows,
            sync_rows=sync_rows,
            checksum_rows=checksum_rows,
            dependency_rows=dependency_rows,
            dependency_summary_rows=dependency_summary_rows,
            maintenance_rows=maintenance_rows,
        )
        logger.info("HTML report dibuat: %s", source_dir / "report.html")
        return 0

    if args.command == "audit-objects":
        from oracle_pg_sync.reports.writer_csv import write_csv

        run_id = new_run_id()
        manifest = RunManifest(
            report_dir=report_dir,
            run_id=run_id,
            command="audit-objects",
            config_file=args.config,
            config=config,
            direction=None,
            dry_run=True,
            tables_requested=[],
            checkpoint_path=str(checkpoint_store.path),
        )
        run_dir = manifest.run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        result = run_object_audit(
            config,
            logger,
            object_types=getattr(args, "types", None),
            include_extension_objects=args.include_extension_objects,
        )
        write_csv(run_dir / "object_inventory.csv", result.inventory_rows)
        write_csv(run_dir / "object_compare.csv", result.compare_rows)
        _copy_log_to_run_dir(report_dir, run_dir)
        manifest_path = manifest.finish(
            result_rows=result.compare_rows,
            report_files=_run_report_files(run_dir, "object_inventory.csv", "object_compare.csv", "logs.txt"),
        )
        logger.info("Manifest dibuat: %s", manifest_path)
        logger.info(
            "Object audit selesai. MATCH=%s MISSING_IN_ORACLE=%s MISSING_IN_POSTGRES=%s",
            _count(result.compare_rows, "MATCH"),
            _count(result.compare_rows, "MISSING_IN_ORACLE"),
            _count(result.compare_rows, "MISSING_IN_POSTGRES"),
        )
        return 0

    if args.command == "dependencies":
        from oracle_pg_sync.reports.writer_csv import write_csv
        from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx
        from oracle_pg_sync.reports.writer_html import write_html_report

        run_id = new_run_id()
        manifest = RunManifest(
            report_dir=report_dir,
            run_id=run_id,
            command="dependencies",
            config_file=args.config,
            config=config,
            direction=None,
            dry_run=True,
            tables_requested=tables,
            checkpoint_path=str(checkpoint_store.path),
        )
        run_dir = manifest.run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        rows = run_table_dependency_audit(config, tables, logger)
        out_path = Path(args.out) if args.out else run_dir / "table_object_dependencies.csv"
        write_csv(out_path, rows)
        summary_rows = _write_dependency_summary(run_dir, rows, [])
        write_central_report_xlsx(
            run_dir / "report.xlsx",
            dependency_rows=rows,
            dependency_summary_rows=summary_rows,
            config_sanitized=sanitize(config),
        )
        write_html_report(
            run_dir / "report.html",
            inventory_rows=[],
            column_diff_rows=[],
            dependency_rows=rows,
            dependency_summary_rows=summary_rows,
            maintenance_rows=[],
        )
        _copy_log_to_run_dir(report_dir, run_dir)
        manifest_path = manifest.finish(
            result_rows=rows,
            dependency_rows=summary_rows,
            report_files=[
                str(out_path),
                *_run_report_files(run_dir, "dependency_summary.csv", "report.xlsx", "report.html", "logs.txt"),
            ],
        )
        logger.info("Manifest dibuat: %s", manifest_path)
        logger.info("Dependency audit selesai. ROWS=%s OUT=%s", len(rows), out_path)
        return 0

    if args.command == "all":
        from oracle_pg_sync.reports import write_audit_reports
        from oracle_pg_sync.reports.writer_csv import write_csv
        from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx, write_rows_xlsx
        from oracle_pg_sync.reports.writer_html import write_html_report
        from oracle_pg_sync.rollback import rollback_run

        run_id = args.resume or new_run_id()
        manifest = RunManifest(
            report_dir=report_dir,
            run_id=run_id,
            command="all",
            config_file=args.config,
            config=config,
            direction=direction,
            dry_run=not args.execute or config.sync.dry_run and not args.execute,
            tables_requested=tables,
            checkpoint_path=str(checkpoint_store.path),
        )
        run_dir = manifest.run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Step 1/3 audit awal")
        pre_audit_result = run_audit(config, tables, logger, workers=args.workers)
        write_csv(run_dir / "pre_inventory_summary.csv", pre_audit_result.inventory_rows)
        write_csv(run_dir / "pre_column_diff.csv", pre_audit_result.column_diff_rows)
        write_csv(run_dir / "pre_type_mismatch.csv", pre_audit_result.type_mismatch_rows)
        dependency_pre_rows = _write_dependency_report(config, tables, logger, run_dir, phase="pre")
        logger.info("Step 2/3 sync direction=%s", direction)
        sync_results = _sync_runner(config, logger, direction).sync_tables(
            tables,
            mode_override=args.mode,
            execute=args.execute,
            force=args.force,
            checkpoint_store=checkpoint_store,
            run_id=run_id,
            resume=bool(args.resume),
            incremental=args.incremental,
            full_refresh=args.full_refresh,
        )
        sync_rows = [result.as_row() for result in sync_results]
        write_csv(run_dir / "sync_result.csv", sync_rows)
        write_rows_xlsx(run_dir / "sync_result.xlsx", sync_rows, sheet_name="sync_result")
        checksum_rows = _checksum_rows_from_results(sync_results, sync_rows)
        if checksum_rows:
            write_csv(run_dir / "validation_checksum.csv", checksum_rows)
            write_rows_xlsx(run_dir / "validation_checksum.xlsx", checksum_rows, sheet_name="checksum")
        maintenance_rows = _run_dependency_maintenance(
            config,
            tables,
            logger,
            run_dir,
            dependency_pre_rows,
            execute=args.execute,
        )
        dependency_post_rows = _write_dependency_report(config, tables, logger, run_dir, phase="post")
        dependency_rows = dependency_pre_rows + dependency_post_rows
        dependency_summary_rows = _write_dependency_summary(run_dir, dependency_rows, maintenance_rows)
        dependency_failed = _dependency_failed(config, dependency_rows + maintenance_rows)
        rollback_rows: list[dict] = []
        sync_failed = any(row["status"] == "FAILED" for row in sync_rows)
        run_failed = dependency_failed or sync_failed
        if args.execute and dependency_failed:
            rollback_rows = rollback_run(config, checkpoint_store, run_id=run_id, logger=logger)
            write_csv(run_dir / "rollback_result.csv", rollback_rows)
        if args.execute and not run_failed:
            _apply_watermark_updates(checkpoint_store, sync_results)
            checkpoint_store.clear_job_failures(job_key)
        elif args.execute:
            checkpoint_store.register_job_failure(
                job_key,
                cooldown_minutes=config.sync.cooldown_minutes,
                error_message=_first_error(sync_rows, maintenance_rows, dependency_failed),
            )
            send_alert(
                config,
                event="dependency_error" if dependency_failed else "failure",
                payload=_alert_payload(
                    run_id=run_id,
                    direction=direction,
                    error=_first_error(sync_rows, maintenance_rows, dependency_failed),
                    failed_tables=[row["table_name"] for row in sync_rows if row.get("status") == "FAILED"],
                ),
                logger=logger,
            )
        metrics_rows = _metrics_rows(sync_results, rollback_rows)
        _write_metrics_json(run_dir, metrics_rows)
        logger.info("Step 3/3 audit ulang dan report")
        audit_result = run_audit(config, tables, logger, workers=args.workers)
        write_audit_reports(
            run_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            dependency_rows=audit_result.dependency_rows,
            sql_suggestions_path=_sql_suggestions_path(run_dir, getattr(args, "sql_out", None)),
            suggest_drop=args.suggest_drop,
            sync_rows=sync_rows,
        )
        _write_run_reports(
            manifest,
            report_dir=report_dir,
            inventory_rows=audit_result.inventory_rows,
            column_diff_rows=audit_result.column_diff_rows,
            type_mismatch_rows=audit_result.type_mismatch_rows,
            sync_rows=sync_rows,
            checksum_rows=checksum_rows,
            dependency_rows=dependency_rows + audit_result.dependency_rows,
            dependency_summary_rows=dependency_summary_rows,
            maintenance_rows=maintenance_rows,
            watermark_rows=checkpoint_store.list_watermarks(),
            checkpoint_rows=checkpoint_store.list_chunks(run_id),
            rollback_rows=rollback_rows,
            timeline_rows=checkpoint_store.list_events(run_id),
            config=config,
            write_central_report_xlsx=write_central_report_xlsx,
            write_html_report=write_html_report,
        )
        manifest_path = manifest.finish(
            result_rows=sync_rows,
            checksum_rows=checksum_rows,
            lob_rows=sync_rows,
            dependency_rows=dependency_summary_rows,
            metrics_rows=metrics_rows,
            rollback_rows=rollback_rows,
            timeline_rows=checkpoint_store.list_events(run_id),
            report_files=_run_report_files(
                run_dir,
                "pre_inventory_summary.csv",
                "pre_column_diff.csv",
                "pre_type_mismatch.csv",
                "sync_result.csv",
                "sync_result.xlsx",
                "validation_checksum.csv",
                "validation_checksum.xlsx",
                "metrics.json",
                "inventory_summary.csv",
                "inventory_summary.xlsx",
                "column_diff.csv",
                "type_mismatch.csv",
                "object_dependency_summary.csv",
                "dependency_pre.csv",
                "dependency_post.csv",
                "dependency_maintenance.csv",
                "dependency_summary.csv",
                "rollback_result.csv",
                "schema_suggestions.sql",
                "report.xlsx",
                "report.html",
                "logs.txt",
            ),
        )
        logger.info("Manifest dibuat: %s", manifest_path)
        return 1 if run_failed else 0

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


def run_table_dependency_audit(config: AppConfig, tables: list[str], logger: logging.Logger) -> list[dict]:
    from oracle_pg_sync.db import oracle, postgres

    rows: list[dict] = []
    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            for table_name in tables:
                table = split_schema_table(table_name, config.postgres.schema)
                logger.info("Dependency audit %s", table.fqname)
                rows.extend(oracle.table_object_dependency_rows(ocur, config.oracle.schema, table.table))
                rows.extend(postgres.table_object_dependency_rows(pcur, table.schema, table.table))
    return rows


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


def _apply_where_override(config: AppConfig, tables: list[str], where: str | None) -> None:
    if not where:
        return
    table_cfg = _single_table_config(config, tables, "--where")
    table_cfg.where = where


def _apply_runtime_table_overrides(args: argparse.Namespace, config: AppConfig, tables: list[str]) -> None:
    _apply_where_override(config, tables, getattr(args, "where", None))
    key_columns = getattr(args, "key_columns", None)
    incremental_column = getattr(args, "incremental_column", None)
    if not key_columns and not incremental_column:
        return
    table_cfg = _single_table_config(
        config,
        tables,
        "--key-columns/--incremental-column",
    )
    if key_columns:
        table_cfg.key_columns = [str(column).lower() for column in key_columns]
    if incremental_column:
        table_cfg.incremental.enabled = True
        table_cfg.incremental.column = str(incremental_column).lower()
        table_cfg.incremental.strategy = getattr(args, "incremental_strategy", "updated_at")
        if getattr(args, "initial_value", None) is not None:
            table_cfg.incremental.initial_value = args.initial_value
        if getattr(args, "overlap_minutes", None) is not None:
            table_cfg.incremental.overlap_minutes = int(args.overlap_minutes)


def _single_table_config(config: AppConfig, tables: list[str], flag_name: str) -> TableConfig:
    if len(tables) != 1:
        raise SystemExit(f"{flag_name} hanya boleh dipakai untuk satu table per command.")
    table_name = tables[0]
    table_cfg = config.table_config(table_name)
    if table_cfg is None:
        table_cfg = TableConfig(name=table_name)
        config.tables.append(table_cfg)
    return table_cfg


def _apply_lob_override(config: AppConfig, strategy: str | None) -> None:
    if not strategy:
        return
    config.lob_strategy.default = "stream" if strategy == "include" else strategy


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


def _latest_run_dir(report_dir: Path) -> Path | None:
    manifests = sorted(report_dir.glob("run_*/manifest.json"), reverse=True)
    return manifests[0].parent if manifests else None


def _print_rows(rows: list[dict]) -> None:
    if not rows:
        print("No rows.")
        return
    fields = list(rows[0].keys())
    print(",".join(fields))
    for row in rows:
        print(",".join(str(row.get(field, "")) for field in fields))


def _count(rows: list[dict], status: str) -> int:
    return sum(1 for row in rows if row.get("status") == status)


def _write_dependency_report(
    config: AppConfig,
    tables: list[str],
    logger: logging.Logger,
    report_dir: Path,
    *,
    phase: str,
) -> list[dict]:
    from oracle_pg_sync.reports.writer_csv import write_csv

    rows = run_table_dependency_audit(config, tables, logger)
    rows = [{**row, "phase": phase} for row in rows]
    write_csv(report_dir / f"dependency_{phase}.csv", rows)
    logger.info("Dependency %s report dibuat rows=%s", phase, len(rows))
    return rows


def _write_dependency_summary(
    report_dir: Path,
    dependency_rows: list[dict],
    maintenance_rows: list[dict],
) -> list[dict]:
    from oracle_pg_sync.reports.writer_csv import write_csv

    rows = summarize_dependency_rows(dependency_rows, maintenance_rows)
    write_csv(report_dir / "dependency_summary.csv", rows)
    return rows


def _dependency_failed(config: AppConfig, rows: list[dict]) -> bool:
    return bool(config.dependency.fail_on_broken_dependency and critical_dependency_rows(rows))


def _run_dependency_maintenance(
    config: AppConfig,
    tables: list[str],
    logger: logging.Logger,
    report_dir: Path,
    dependency_rows: list[dict],
    *,
    execute: bool,
) -> list[dict]:
    from oracle_pg_sync.db import oracle, postgres
    from oracle_pg_sync.reports.writer_csv import write_csv

    if not execute:
        write_csv(report_dir / "dependency_maintenance.csv", [])
        return []
    rows: list[dict] = []
    try:
        with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
            with ocon.cursor() as ocur, pcon.cursor() as pcur:
                attempts = max(
                    1,
                    int(getattr(config.dependency, "max_attempts", 0) or config.dependency.max_recompile_attempts or 1),
                )
                remaining_invalid = oracle.invalid_object_rows(ocur, config.oracle.schema) if hasattr(ocur, "execute") else [{}]
                for attempt in range(1, attempts + 1):
                    if config.dependency.refresh_postgres_mview:
                        rows.extend({**row, "attempt": attempt} for row in postgres.refresh_materialized_views(pcur, dependency_rows))
                    attempt_rows = []
                    if config.dependency.auto_recompile_oracle:
                        attempt_rows = oracle.compile_invalid_objects(ocur, config.oracle.schema)
                    ocon.commit()
                    post_attempt_invalid = oracle.invalid_object_rows(ocur, config.oracle.schema) if hasattr(ocur, "execute") else []
                    remaining_keys = {
                        (str(item.get("object_schema")), str(item.get("object_type")), str(item.get("object_name")))
                        for item in post_attempt_invalid
                    }
                    for row in attempt_rows:
                        key = (str(row.get("object_schema")), str(row.get("object_type")), str(row.get("object_name")))
                        rows.append(
                            {
                                **row,
                                "attempt": attempt,
                                "maintenance_status": "fixed" if key not in remaining_keys else "failed",
                                "validation_status": "valid" if key not in remaining_keys else "invalid",
                            }
                        )
                    remaining_invalid = post_attempt_invalid
                    if not remaining_invalid:
                        break
                if remaining_invalid:
                    rows.extend(
                        {
                            **row,
                            "attempt": attempts,
                            "maintenance_status": "failed",
                            "validation_status": "invalid",
                            "error_message": row.get("status") or "still invalid after repair loop",
                        }
                        for row in remaining_invalid
                    )
                rows.extend(postgres.validate_dependent_objects(pcur, dependency_rows))
    except Exception as exc:
        logger.exception("Dependency maintenance failed")
        rows.append({
            "source_db": "",
            "object_schema": "",
            "object_type": "DEPENDENCY_MAINTENANCE",
            "object_name": "",
            "maintenance_status": "failed",
            "error_message": str(exc),
        })
    write_csv(report_dir / "dependency_maintenance.csv", rows)
    logger.info("Dependency maintenance selesai rows=%s tables=%s", len(rows), len(tables))
    return rows


def _checksum_rows_from_results(results: list, fallback_rows: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for result in results:
        rows.extend(getattr(result, "checksum_rows", []) or [])
    if rows:
        return rows
    return [row for row in fallback_rows if row.get("checksum_status")]


def _apply_watermark_updates(checkpoint_store: CheckpointStore, results: list[Any]) -> None:
    for result in results:
        candidate = getattr(result, "watermark_candidate", None)
        if not candidate:
            continue
        checkpoint_store.set_watermark(
            direction=candidate.direction,
            table_name=candidate.table_name,
            strategy=candidate.strategy,
            column_name=candidate.column_name,
            value=candidate.value,
        )


def _metrics_rows(results: list[Any], rollback_rows: list[dict] | None = None) -> list[dict]:
    rollback_rows = rollback_rows or []
    metrics: list[dict] = []
    for result in results:
        metrics.append(
            {
                "table_name": getattr(result, "table_name", ""),
                "mode": getattr(result, "mode", ""),
                "status": getattr(result, "status", ""),
                "elapsed_seconds": round(float(getattr(result, "elapsed_seconds", 0) or 0), 3),
                "rows_loaded": int(getattr(result, "rows_loaded", 0) or 0),
                "rows_per_second": getattr(result, "rows_per_second", None),
                "bytes_processed": int(getattr(result, "bytes_processed", 0) or 0),
                "bytes_per_second": getattr(result, "bytes_per_second", None),
                "lob_bytes_processed": int(getattr(result, "lob_bytes_processed", 0) or 0),
                "error_rate": 1.0 if getattr(result, "status", "") == "FAILED" else 0.0,
                "rollback_available": bool(getattr(result, "rollback_available", False)),
                "rollback_action": getattr(result, "rollback_action", ""),
            }
        )
    if rollback_rows:
        metrics.append(
            {
                "table_name": "__rollback__",
                "mode": "",
                "status": "SUCCESS" if all(row.get("status") == "SUCCESS" for row in rollback_rows) else "FAILED",
                "elapsed_seconds": 0,
                "rows_loaded": 0,
                "rows_per_second": None,
                "bytes_processed": 0,
                "bytes_per_second": None,
                "lob_bytes_processed": 0,
                "error_rate": 0.0,
                "rollback_available": True,
                "rollback_action": "automatic",
            }
        )
    return metrics


def _write_metrics_json(run_dir: Path, metrics_rows: list[dict]) -> None:
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "tables": metrics_rows,
        "slow_tables": [row for row in metrics_rows if float(row.get("elapsed_seconds") or 0) >= 300],
    }
    (run_dir / "metrics.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _first_error(rows: list[dict], maintenance_rows: list[dict], dependency_failed: bool) -> str:
    for row in rows:
        if row.get("status") == "FAILED" and row.get("message"):
            return str(row["message"])
    for row in maintenance_rows:
        if row.get("error_message"):
            return str(row["error_message"])
    return "dependency validation failed" if dependency_failed else "sync failed"


def _alert_payload(*, run_id: str, direction: str | None, error: str, failed_tables: list[str]) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "direction": direction or "",
        "error": error,
        "failed_tables": failed_tables,
    }


def _job_key(config: AppConfig, args: argparse.Namespace, direction: str | None, tables: list[str]) -> str:
    key = getattr(config.job, "name", "") or Path(getattr(args, "config", "config.yaml")).stem
    return f"{key}:{getattr(args, 'command', '')}:{direction or ''}:{','.join(sorted(tables))}"


def _simulate_sync(
    config: AppConfig,
    tables: list[str],
    logger: logging.Logger,
    *,
    direction: str | None,
    mode: str | None,
) -> int:
    from oracle_pg_sync.db import oracle, postgres
    from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
    from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata

    rows: list[dict[str, Any]] = []
    with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
        with ocon.cursor() as ocur, pcon.cursor() as pcur:
            dependency_rows = run_table_dependency_audit(config, tables, logger)
            dependency_map: dict[str, int] = {}
            for row in dependency_rows:
                key = str(row.get("table_name") or "")
                dependency_map[key] = dependency_map.get(key, 0) + 1
            for table_name in tables:
                table = split_schema_table(table_name, config.postgres.schema)
                oracle_meta = fetch_oracle_metadata(ocur, owner=config.oracle.schema, table=table.table, fast_count=True)
                pg_meta = fetch_pg_metadata(pcur, schema=table.schema, table=table.table, fast_count=True)
                estimated_rows = int(oracle_meta.row_count or 0)
                estimated_seconds = round(estimated_rows / 10000, 3) if estimated_rows else 0
                relation_size = postgres.total_relation_size_bytes(pcur, table.schema, table.table) or 0
                effective_mode = mode or (config.table_config(table_name).mode if config.table_config(table_name) else config.sync.default_mode)
                risk = "low"
                if dependency_map.get(table.fqname, 0) > 10 or relation_size > 1024**3:
                    risk = "high"
                elif relation_size > 100 * 1024**2 or dependency_map.get(table.fqname, 0) > 0:
                    risk = "medium"
                rows.append(
                    {
                        "table_name": table.fqname,
                        "direction": direction or "",
                        "mode": effective_mode,
                        "estimated_rows": estimated_rows,
                        "estimated_duration_seconds": estimated_seconds,
                        "affected_tables": table.fqname,
                        "dependency_impact": dependency_map.get(table.fqname, 0),
                        "risk_level": risk,
                    }
                )
    fields = list(rows[0].keys()) if rows else []
    if fields:
        print(",".join(fields))
        for row in rows:
            print(",".join(str(row.get(field, "")) for field in fields))
    return 0


def _apply_profile(args: argparse.Namespace) -> None:
    profile = getattr(args, "profile", None)
    if profile == "daily":
        if getattr(args, "mode", None) is None:
            args.mode = "truncate_safe"
        args.full_refresh = True
    elif profile == "every_5min":
        if getattr(args, "mode", None) is None:
            args.mode = "incremental_safe"
        args.incremental = True


def _rotate_log(path: Path, *, max_bytes: int) -> None:
    if max_bytes <= 0 or not path.exists() or path.stat().st_size <= max_bytes:
        return
    token = time.strftime("%Y%m%d_%H%M%S")
    path.rename(path.with_name(f"{path.stem}_{token}{path.suffix}"))


def _maybe_acquire_lock(args: argparse.Namespace, logger: logging.Logger):
    if getattr(args, "command", "") not in {"sync", "all"} or getattr(args, "no_lock", False):
        return None
    if any(getattr(args, attr, None) for attr in ("list_runs", "reset_checkpoint", "watermark_status", "reset_watermark")):
        return None
    path = Path(getattr(args, "lock_file", "reports/sync.lock"))
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(f"Another sync job is running; lock file: {path}")
    handle.write(f"pid={os.getpid()} started_at={time.strftime('%Y-%m-%dT%H:%M:%S')}\n")
    handle.flush()
    atexit.register(_release_lock, handle, path)
    logger.info("Lock acquired: %s", path)
    return handle


def _release_lock(handle, path: Path) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _write_run_reports(
    manifest: RunManifest,
    *,
    report_dir: Path,
    inventory_rows: list[dict] | None = None,
    column_diff_rows: list[dict] | None = None,
    type_mismatch_rows: list[dict] | None = None,
    sync_rows: list[dict],
    checksum_rows: list[dict],
    dependency_rows: list[dict] | None = None,
    dependency_summary_rows: list[dict] | None = None,
    maintenance_rows: list[dict] | None = None,
    watermark_rows: list[dict] | None = None,
    checkpoint_rows: list[dict] | None = None,
    rollback_rows: list[dict] | None = None,
    timeline_rows: list[dict] | None = None,
    config: AppConfig,
    write_central_report_xlsx,
    write_html_report,
) -> None:
    run_dir = manifest.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    write_central_report_xlsx(
        run_dir / "report.xlsx",
        inventory_rows=inventory_rows or [],
        column_diff_rows=column_diff_rows or [],
        type_mismatch_rows=type_mismatch_rows or [],
        sync_rows=sync_rows,
        checksum_rows=checksum_rows,
        dependency_rows=dependency_rows or [],
        dependency_summary_rows=dependency_summary_rows or [],
        maintenance_rows=maintenance_rows or [],
        watermark_rows=watermark_rows or [],
        checkpoint_rows=checkpoint_rows or [],
        rollback_rows=rollback_rows or [],
        timeline_rows=timeline_rows or [],
        config_sanitized=sanitize(config),
    )
    write_html_report(
        run_dir / "report.html",
        inventory_rows=inventory_rows or [],
        column_diff_rows=column_diff_rows or [],
        sync_rows=sync_rows,
        checksum_rows=checksum_rows,
        dependency_rows=dependency_rows or [],
        dependency_summary_rows=dependency_summary_rows or [],
        maintenance_rows=maintenance_rows or [],
        rollback_rows=rollback_rows or [],
        timeline_rows=timeline_rows or [],
    )
    _copy_log_to_run_dir(report_dir, run_dir)


def _write_audit_run_reports(
    manifest: RunManifest,
    *,
    report_dir: Path,
    inventory_rows: list[dict] | None = None,
    column_diff_rows: list[dict] | None = None,
    type_mismatch_rows: list[dict] | None = None,
    dependency_rows: list[dict] | None = None,
    config: AppConfig,
    write_central_report_xlsx,
) -> list[dict]:
    from oracle_pg_sync.reports.writer_html import write_html_report

    run_dir = manifest.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    dependency_summary_rows = summarize_dependency_rows(dependency_rows or [], [])
    write_central_report_xlsx(
        run_dir / "report.xlsx",
        inventory_rows=inventory_rows or [],
        column_diff_rows=column_diff_rows or [],
        type_mismatch_rows=type_mismatch_rows or [],
        sync_rows=[],
        checksum_rows=[],
        dependency_rows=dependency_rows or [],
        dependency_summary_rows=dependency_summary_rows,
        config_sanitized=sanitize(config),
    )
    write_html_report(
        run_dir / "report.html",
        inventory_rows=inventory_rows or [],
        column_diff_rows=column_diff_rows or [],
        sync_rows=[],
        checksum_rows=[],
        dependency_rows=dependency_rows or [],
        dependency_summary_rows=dependency_summary_rows,
        maintenance_rows=[],
    )
    _copy_log_to_run_dir(report_dir, run_dir)
    return dependency_summary_rows


def _copy_log_to_run_dir(report_dir: Path, run_dir: Path) -> None:
    log_path = report_dir / "sync.log"
    if log_path.exists():
        shutil.copyfile(log_path, run_dir / "logs.txt")
    else:
        (run_dir / "logs.txt").write_text("", encoding="utf-8")


def _run_report_files(run_dir: Path, *names: str) -> list[str]:
    return [str(run_dir / name) for name in names if (run_dir / name).exists()]


if __name__ == "__main__":
    raise SystemExit(main())
