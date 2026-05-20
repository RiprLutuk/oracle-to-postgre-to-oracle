#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = ["source_run"]
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def append_source(rows: list[dict[str, str]], run_name: str) -> list[dict[str, object]]:
    return [{"source_run": run_name, **row} for row in rows]


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(
            "Usage: centralize_daily_runs.py PROFILE YYYYMMDD RUN_DIR [RUN_DIR ...]",
            file=sys.stderr,
        )
        return 2

    profile = argv[1]
    day = argv[2]
    repo = Path.cwd()
    out = repo / "reports" / "daily_runs" / profile / day
    raw = out / "raw_runs"
    out.mkdir(parents=True, exist_ok=True)
    raw.mkdir(parents=True, exist_ok=True)

    runs = [Path(value).resolve() for value in argv[3:]]
    summaries: list[dict[str, object]] = []
    rowcount_rows: list[dict[str, object]] = []
    sync_rows: list[dict[str, object]] = []
    existing_summary = read_csv(out / "run_summary.csv")
    existing_source_runs = {row.get("source_run") for row in existing_summary if row.get("source_run")}

    with (out / "logs.txt").open("a", encoding="utf-8") as log_out:
        for run in runs:
            src = run if run.exists() else raw / run.name
            if not src.exists():
                summaries.append({"source_run": run.name, "status": "missing"})
                continue
            if run.name in existing_source_runs:
                if run.exists():
                    dst = raw / run.name
                    if dst.exists():
                        shutil.rmtree(dst)
                    shutil.move(str(run), str(dst))
                continue

            manifest = {}
            manifest_path = src / "manifest.json"
            if manifest_path.exists():
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except Exception as exc:  # pragma: no cover - defensive report helper
                    manifest = {"manifest_error": str(exc)}

            summaries.append(
                {
                    "source_run": run.name,
                    "status": "archived" if src.parent == raw else "found",
                    "command": manifest.get("command", ""),
                    "direction": manifest.get("direction", ""),
                    "started_at": manifest.get("started_at", ""),
                    "finished_at": manifest.get("finished_at", ""),
                    "duration_seconds": manifest.get("duration_seconds", ""),
                    "tables_processed": manifest.get("tables_processed", ""),
                    "tables_failed": manifest.get("tables_failed", ""),
                }
            )

            log_out.write(f"===== {run.name} =====\n")
            log_path = src / "logs.txt"
            if log_path.exists():
                log_out.write(log_path.read_text(encoding="utf-8", errors="replace"))
                log_out.write("\n")
            else:
                log_out.write("(logs.txt missing)\n")
            log_out.write("\n")

            rowcount_rows.extend(append_source(read_csv(src / "rowcount_validation.csv"), run.name))
            sync_rows.extend(append_source(read_csv(src / "sync_result.csv"), run.name))

            if run.exists():
                dst = raw / run.name
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.move(str(run), str(dst))

    existing_rowcount = read_csv(out / "rowcount_validation.csv")
    existing_sync = read_csv(out / "sync_result.csv")

    write_rows(out / "run_summary.csv", [*existing_summary, *summaries])
    write_rows(out / "rowcount_validation.csv", [*existing_rowcount, *rowcount_rows])
    write_rows(out / "sync_result.csv", [*existing_sync, *sync_rows])

    (out / "manifest.json").write_text(
        json.dumps(
            {
                "profile": profile,
                "date": day,
                "latest_append_count": len(runs),
                "daily_folder": str(out),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
