from __future__ import annotations

import sys
from pathlib import Path

from oracle_pg_sync.checkpoint import CheckpointStore
from oracle_pg_sync.cli import main as cli_main
from oracle_pg_sync.config import load_config


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print("Usage: ops audit|sync|resume|status|watermarks|reset-watermark|validate|report ...")
        return 2

    command = args[0]
    rest = args[1:]
    if command in {"audit", "sync"}:
        return cli_main([command, *rest])
    if command == "report":
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
