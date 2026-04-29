from __future__ import annotations

import shutil
import sys
from pathlib import Path

from oracle_pg_sync.checkpoint import CheckpointStore
from oracle_pg_sync.cli import main as cli_main
from oracle_pg_sync.config import load_config


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        _print_usage()
        return 2
    if args[0] in {"-h", "--help"}:
        _print_usage()
        return 0

    command = args[0]
    rest = args[1:]
    if command in {"audit", "sync"}:
        return cli_main([command, *rest])
    if command == "doctor":
        return _doctor(rest)
    if command == "dependencies":
        return _dependencies(rest)
    if command == "analyze":
        return _analyze(rest)
    if command == "report":
        if rest and rest[0] == "latest":
            _print_latest_report(rest[1:])
            return 0
        if rest and not rest[0].startswith("-"):
            rest = rest[1:]
        return cli_main(["report", *rest])
    if command == "validate":
        rest = _expand_bare_lob_flag(rest)
        return cli_main(["sync", *rest])
    if command == "resume":
        run_id = rest[0] if rest and not rest[0].startswith("-") else _latest_failed_run_id(rest)
        tail = rest[1:] if rest and not rest[0].startswith("-") else rest
        if not run_id:
            print("No failed run found to resume.")
            return 1
        return cli_main(["sync", "--resume", run_id, *tail])
    if command == "watermarks":
        return cli_main(["sync", "--watermark-status", *rest])
    if command == "reset-watermark":
        if not rest or rest[0].startswith("-"):
            print("Usage: ops reset-watermark TABLE [--config config.yaml]")
            return 2
        return cli_main(["sync", "--reset-watermark", rest[0], *rest[1:]])
    if command == "status":
        _print_status(rest)
        return 0
    print(f"Unsupported ops command: {command}")
    return 2


def _print_usage() -> None:
    print("Usage: ops audit|sync|resume|status|watermarks|reset-watermark|validate|report|doctor ...")
    print("")
    print("Common:")
    print("  ops audit --config config.yaml")
    print("  ops sync --config config.yaml")
    print("  ops sync --go --config config.yaml")
    print("  ops resume [RUN_ID] --config config.yaml")
    print("  ops status --config config.yaml")
    print("  ops report latest --config config.yaml")
    print("  ops doctor --config config.yaml")
    print("  ops dependencies check --config config.yaml")
    print("  ops dependencies repair --config config.yaml")
    print("  ops analyze lob --config config.yaml")


def _doctor(args: list[str]) -> int:
    rows: list[tuple[str, str, str]] = []
    critical = False
    try:
        config = load_config(_config_path(args))
        rows.append(("config", "OK", "config loaded"))
    except Exception as exc:
        _print_check_rows([("config", "ERROR", str(exc))])
        return 1

    offline = "--offline" in args
    table_source = f"tables_file={config.tables_file}" if config.tables_file else "inline tables"
    rows.append(("table_config", "OK", f"{len(config.tables)} tables configured from {table_source}"))
    rows.append(("checkpoint_db", "OK", str(config.sync.checkpoint_dir)))
    lock_path = _arg_value(args, "--lock-file") or "reports/sync.lock"
    rows.append(("job_lock_file", "OK", lock_path))
    free_gib = shutil.disk_usage(Path(config.reports.output_dir).resolve().parent).free / (1024**3)
    rows.append(("disk_space", "OK" if free_gib >= 1 else "WARNING", f"{free_gib:.1f} GiB free"))
    if offline:
        rows.append(("oracle_connection", "WARNING", "skipped by --offline"))
        rows.append(("postgres_connection", "WARNING", "skipped by --offline"))
        _print_check_rows(rows)
        return 1 if critical else 0

    rows.append(_check_oracle(config))
    rows.extend(_check_postgres(config))
    critical = critical or any(row[1] == "ERROR" for row in rows)
    _print_check_rows(rows)
    return 1 if critical else 0


def _dependencies(args: list[str]) -> int:
    if not args or args[0] in {"-h", "--help"}:
        print("Usage: ops dependencies check|repair [--config config.yaml] [--tables TABLE ...]")
        return 0 if args and args[0] in {"-h", "--help"} else 2
    action = args[0]
    rest = args[1:]
    if action == "check":
        return cli_main(["dependencies", *rest])
    if action == "repair":
        return _repair_dependencies(rest)
    print(f"Unsupported dependencies action: {action}")
    return 2


def _analyze(args: list[str]) -> int:
    if not args or args[0] in {"-h", "--help"}:
        print("Usage: ops analyze lob [--config config.yaml] [--tables TABLE ...]")
        return 0 if args and args[0] in {"-h", "--help"} else 2
    if args[0] != "lob":
        print(f"Unsupported analyze target: {args[0]}")
        return 2
    return _analyze_lob(args[1:])


def _repair_dependencies(args: list[str]) -> int:
    from oracle_pg_sync.checkpoint import new_run_id
    from oracle_pg_sync.cli import _resolve_tables, _run_dependency_maintenance, _write_dependency_report
    from oracle_pg_sync.manifest import RunManifest, sanitize
    from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx
    from oracle_pg_sync.reports.writer_html import write_html_report
    from oracle_pg_sync.utils.logging import setup_logging

    config = load_config(_config_path(args))
    report_dir = Path(config.reports.output_dir)
    logger = setup_logging(report_dir)
    tables = _resolve_tables(
        config,
        _tables_arg(args),
        tables_file=_arg_value(args, "--tables-file"),
        limit=_int_arg(args, "--limit"),
    )
    if not tables:
        print("Tidak ada table target. Isi config.tables atau pakai --tables.")
        return 2
    run_id = new_run_id()
    manifest = RunManifest(
        report_dir=report_dir,
        run_id=run_id,
        command="dependencies repair",
        config_file=_config_path(args),
        config=config,
        direction=None,
        dry_run=False,
        tables_requested=tables,
        checkpoint_path=str(config.sync.checkpoint_dir),
    )
    run_dir = manifest.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    pre_rows = _write_dependency_report(config, tables, logger, run_dir, phase="pre")
    maintenance_rows = _run_dependency_maintenance(config, tables, logger, run_dir, pre_rows, execute=True)
    post_rows = _write_dependency_report(config, tables, logger, run_dir, phase="post")
    write_central_report_xlsx(
        run_dir / "report.xlsx",
        dependency_rows=pre_rows + post_rows,
        maintenance_rows=maintenance_rows,
        config_sanitized=sanitize(config),
    )
    write_html_report(
        run_dir / "report.html",
        inventory_rows=[],
        column_diff_rows=[],
        dependency_rows=pre_rows + post_rows,
        maintenance_rows=maintenance_rows,
    )
    manifest.finish(
        result_rows=maintenance_rows,
        report_files=[
            str(run_dir / "dependency_pre.csv"),
            str(run_dir / "dependency_post.csv"),
            str(run_dir / "dependency_maintenance.csv"),
            str(run_dir / "report.xlsx"),
            str(run_dir / "report.html"),
        ],
    )
    failed = any(
        str(row.get("maintenance_status") or row.get("compile_status") or row.get("validation_status")).lower()
        in {"failed", "missing"}
        for row in maintenance_rows
    )
    print(f"report_path,{run_dir / 'report.html'}")
    return 1 if failed else 0


def _analyze_lob(args: list[str]) -> int:
    from oracle_pg_sync.checkpoint import new_run_id
    from oracle_pg_sync.cli import _resolve_tables
    from oracle_pg_sync.lob_analysis import analyze_lob_columns
    from oracle_pg_sync.manifest import RunManifest, sanitize
    from oracle_pg_sync.reports.writer_csv import write_csv
    from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx
    from oracle_pg_sync.reports.writer_html import write_html_report
    from oracle_pg_sync.utils.logging import setup_logging

    config = load_config(_config_path(args))
    report_dir = Path(config.reports.output_dir)
    logger = setup_logging(report_dir)
    tables = _resolve_tables(
        config,
        _tables_arg(args),
        tables_file=_arg_value(args, "--tables-file"),
        limit=_int_arg(args, "--limit"),
    )
    if not tables:
        print("Tidak ada table target. Isi config.tables atau pakai --tables.")
        return 2
    run_id = new_run_id()
    manifest = RunManifest(
        report_dir=report_dir,
        run_id=run_id,
        command="analyze lob",
        config_file=_config_path(args),
        config=config,
        direction=None,
        dry_run=True,
        tables_requested=tables,
        checkpoint_path=str(config.sync.checkpoint_dir),
    )
    run_dir = manifest.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = analyze_lob_columns(config, tables, logger)
    write_csv(run_dir / "lob_analysis.csv", rows)
    write_central_report_xlsx(
        run_dir / "report.xlsx",
        lob_rows=rows,
        config_sanitized=sanitize(config),
    )
    write_html_report(
        run_dir / "report.html",
        inventory_rows=[],
        column_diff_rows=[],
        lob_rows=rows,
    )
    manifest.finish(
        result_rows=rows,
        lob_rows=rows,
        report_files=[
            str(run_dir / "lob_analysis.csv"),
            str(run_dir / "report.xlsx"),
            str(run_dir / "report.html"),
        ],
    )
    print(f"report_path,{run_dir / 'report.html'}")
    print(f"lob_analysis_path,{run_dir / 'lob_analysis.csv'}")
    return 0


def _check_oracle(config) -> tuple[str, str, str]:
    try:
        from oracle_pg_sync.db import oracle

        with oracle.connect(config.oracle) as con:
            with con.cursor() as cur:
                cur.execute("SELECT 1 FROM DUAL")
                cur.fetchone()
        return ("oracle_connection", "OK", "connected")
    except Exception as exc:
        return ("oracle_connection", "ERROR", str(exc))


def _check_postgres(config) -> list[tuple[str, str, str]]:
    try:
        from oracle_pg_sync.db import postgres

        with postgres.connect(config.postgres, autocommit=True) as con:
            with con.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'")
                pgcrypto = cur.fetchone() is not None
                cur.execute("SELECT has_schema_privilege(current_user, %s, 'USAGE')", (config.postgres.schema,))
                schema_usage = bool(cur.fetchone()[0])
        ext_status = "OK" if pgcrypto else "WARNING"
        ext_message = "pgcrypto installed" if pgcrypto else "pgcrypto extension not installed"
        privilege_status = "OK" if schema_usage else "ERROR"
        privilege_message = (
            f"USAGE on schema {config.postgres.schema}"
            if schema_usage
            else f"missing USAGE on schema {config.postgres.schema}"
        )
        return [
            ("postgres_connection", "OK", "connected"),
            ("postgres_pgcrypto", ext_status, ext_message),
            ("postgres_privileges", privilege_status, privilege_message),
        ]
    except Exception as exc:
        return [
            ("postgres_connection", "ERROR", str(exc)),
            ("postgres_pgcrypto", "WARNING", "skipped"),
            ("postgres_privileges", "WARNING", "skipped"),
        ]


def _print_check_rows(rows: list[tuple[str, str, str]]) -> None:
    print("check,status,message")
    for name, status, message in rows:
        print(f"{name},{status},{message}")


def _arg_value(args: list[str], flag: str) -> str | None:
    for idx, arg in enumerate(args):
        if arg == flag and idx + 1 < len(args):
            return args[idx + 1]
        if arg.startswith(flag + "="):
            return arg.split("=", 1)[1]
    return None


def _int_arg(args: list[str], flag: str) -> int | None:
    value = _arg_value(args, flag)
    return int(value) if value not in (None, "") else None


def _tables_arg(args: list[str]) -> list[str] | None:
    if "--tables" not in args:
        return None
    idx = args.index("--tables") + 1
    values: list[str] = []
    while idx < len(args) and not args[idx].startswith("-"):
        values.append(args[idx])
        idx += 1
    return values or None


def _latest_failed_run_id(args: list[str]) -> str:
    config = load_config(_config_path(args))
    store = CheckpointStore(config.sync.checkpoint_dir)
    for row in store.list_runs():
        if row.get("status") == "failed":
            return str(row.get("run_id") or "")
    return ""


def _print_status(args: list[str]) -> None:
    config = load_config(_config_path(args))
    report_dir = Path(config.reports.output_dir)
    store = CheckpointStore(config.sync.checkpoint_dir)
    runs = store.list_runs()
    latest = runs[0] if runs else {}
    manifests = sorted(report_dir.glob("run_*/manifest.json"), reverse=True)
    print(f"run_id,{latest.get('run_id', '')}")
    print(f"status,{latest.get('status', '')}")
    print(f"direction,{latest.get('direction', '')}")
    print(f"started_at,{latest.get('started_at', '')}")
    print(f"finished_at,{latest.get('finished_at', '')}")
    print(f"report_path,{manifests[0].parent if manifests else report_dir}")


def _print_latest_report(args: list[str]) -> None:
    config = load_config(_config_path(args))
    report_dir = Path(config.reports.output_dir)
    manifests = sorted(report_dir.glob("run_*/manifest.json"), reverse=True)
    if not manifests:
        print(f"report_path,{report_dir / 'report.html'}")
        return
    run_dir = manifests[0].parent
    print(f"report_path,{run_dir / 'report.html'}")
    print(f"excel_path,{run_dir / 'report.xlsx'}")
    print(f"manifest_path,{run_dir / 'manifest.json'}")


def _config_path(args: list[str]) -> str:
    for idx, arg in enumerate(args):
        if arg == "--config" and idx + 1 < len(args):
            return args[idx + 1]
        if arg.startswith("--config="):
            return arg.split("=", 1)[1]
    return "config.yaml"


def _expand_bare_lob_flag(args: list[str]) -> list[str]:
    result: list[str] = []
    idx = 0
    while idx < len(args):
        item = args[idx]
        if item == "--lob" and (idx + 1 == len(args) or args[idx + 1].startswith("-")):
            result.extend(["--lob", "stream"])
            idx += 1
            continue
        result.append(item)
        idx += 1
    return result


if __name__ == "__main__":
    raise SystemExit(main())
