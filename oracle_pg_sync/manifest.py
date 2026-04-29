from __future__ import annotations

import hashlib
import json
import re
import subprocess
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from oracle_pg_sync.checkpoint import utc_now
from oracle_pg_sync.config import AppConfig


SECRET_KEYS = {"password", "passwd", "pwd", "secret", "token", "key"}


class RunManifest:
    def __init__(
        self,
        *,
        report_dir: Path,
        run_id: str,
        command: str,
        config_file: str,
        config: AppConfig,
        direction: str | None,
        dry_run: bool,
        tables_requested: list[str],
        checkpoint_path: str | None = None,
    ) -> None:
        self.report_dir = Path(report_dir)
        self.run_id = run_id
        self.started = time.time()
        self.data: dict[str, Any] = {
            "run_id": run_id,
            "command": command,
            "started_at": utc_now(),
            "finished_at": "",
            "duration_seconds": 0,
            "git_commit": git_commit(),
            "config_file": config_file,
            "config_hash": config_hash(config_file),
            "source_db_label": source_label(config, direction),
            "target_db_label": target_label(config, direction),
            "direction": direction,
            "dry_run": dry_run,
            "tables_requested": tables_requested,
            "tables_processed": 0,
            "tables_success": 0,
            "tables_failed": 0,
            "rows_read": 0,
            "rows_written": 0,
            "validation_summary": {},
            "checksum_summary": {},
            "lob_summary": {},
            "dependency_summary": {},
            "checkpoint_path": checkpoint_path or "",
            "report_files": [],
            "errors": [],
        }

    @property
    def run_dir(self) -> Path:
        safe_run = "".join(ch for ch in self.run_id if ch.isalnum() or ch in "-_")
        return self.report_dir / f"run_{time.strftime('%Y%m%d_%H%M%S', time.localtime(self.started))}_{safe_run}"

    def finish(
        self,
        *,
        result_rows: list[dict] | None = None,
        checksum_rows: list[dict] | None = None,
        lob_rows: list[dict] | None = None,
        dependency_rows: list[dict] | None = None,
        metrics_rows: list[dict] | None = None,
        rollback_rows: list[dict] | None = None,
        timeline_rows: list[dict] | None = None,
        report_files: list[str] | None = None,
        errors: list[str] | None = None,
    ) -> Path:
        result_rows = result_rows or []
        checksum_rows = checksum_rows or []
        lob_rows = lob_rows or []
        dependency_rows = dependency_rows or []
        metrics_rows = metrics_rows or []
        rollback_rows = rollback_rows or []
        timeline_rows = timeline_rows or []
        self.data["finished_at"] = utc_now()
        self.data["duration_seconds"] = round(time.time() - self.started, 3)
        self.data["tables_processed"] = len(result_rows)
        self.data["tables_success"] = sum(1 for row in result_rows if row.get("status") in {"SUCCESS", "DRY_RUN"})
        self.data["tables_failed"] = sum(1 for row in result_rows if row.get("status") == "FAILED")
        self.data["rows_written"] = sum(int(row.get("rows_loaded") or 0) for row in result_rows)
        self.data["rows_read"] = self.data["rows_written"]
        self.data["checksum_summary"] = {
            "total": len(checksum_rows),
            "match": sum(1 for row in checksum_rows if row.get("status") == "MATCH"),
            "mismatch": sum(1 for row in checksum_rows if row.get("status") == "MISMATCH"),
        }
        self.data["lob_summary"] = summarize_lob_rows(lob_rows)
        self.data["dependency_summary"] = summarize_dependency_manifest(dependency_rows)
        self.data["metrics_summary"] = summarize_metrics(metrics_rows)
        self.data["rollback_summary"] = summarize_rollback(rollback_rows)
        self.data["failure_timeline"] = timeline_rows
        self.data["report_files"] = report_files or []
        self.data["errors"] = errors or [str(row.get("message")) for row in result_rows if row.get("status") == "FAILED"]
        self.run_dir.mkdir(parents=True, exist_ok=True)
        path = self.run_dir / "manifest.json"
        path.write_text(json.dumps(sanitize(self.data), indent=2, sort_keys=True), encoding="utf-8")
        return path


def sanitize(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if any(secret in str(key).lower() for secret in SECRET_KEYS):
                result[key] = "****" if item else ""
            else:
                result[key] = sanitize(item)
        return result
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    return value


def config_hash(config_file: str) -> str:
    path = Path(config_file)
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return ""


def source_label(config: AppConfig, direction: str | None) -> str:
    if direction == "postgres-to-oracle":
        return f"postgres://{config.postgres.host or ''}/{config.postgres.database or ''}"
    return f"oracle://{sanitize_connection_text(config.oracle.host or config.oracle.dsn or '')}/{config.oracle.schema or ''}"


def target_label(config: AppConfig, direction: str | None) -> str:
    if direction == "postgres-to-oracle":
        return f"oracle://{sanitize_connection_text(config.oracle.host or config.oracle.dsn or '')}/{config.oracle.schema or ''}"
    return f"postgres://{config.postgres.host or ''}/{config.postgres.database or ''}"


def sanitize_connection_text(value: str) -> str:
    return re.sub(r"//([^:/@\s]+):([^@\s]+)@", r"//****:****@", value)


def summarize_lob_rows(rows: list[dict]) -> dict[str, Any]:
    detected = _split_lob_field(rows, "lob_columns_detected")
    synced = _split_lob_field(rows, "lob_columns_synced")
    return {
        "lob_columns_detected": len(detected),
        "lob_columns_synced": len(synced),
        "lob_columns_skipped": len(_split_lob_field(rows, "lob_columns_skipped")),
        "lob_columns_nullified": len(_split_lob_field(rows, "lob_columns_nullified")),
        "lob_types": sorted(_split_map_values(rows, "lob_type")),
        "target_types": sorted(_split_map_values(rows, "lob_target_type")),
        "validation_modes": sorted(_split_map_values(rows, "lob_validation_mode")),
    }


def summarize_dependency_manifest(rows: list[dict]) -> dict[str, Any]:
    return {
        "total": len(rows),
        "broken": sum(1 for row in rows if int(row.get("broken_count") or 0) > 0),
        "invalid": sum(int(row.get("invalid_count") or 0) for row in rows),
        "missing": sum(int(row.get("missing_count") or 0) for row in rows),
        "failed": sum(int(row.get("failed_count") or 0) for row in rows),
    }


def summarize_metrics(rows: list[dict]) -> dict[str, Any]:
    return {
        "total_bytes_processed": sum(int(row.get("bytes_processed") or 0) for row in rows),
        "total_lob_bytes_processed": sum(int(row.get("lob_bytes_processed") or 0) for row in rows),
        "slow_tables": [row.get("table_name") for row in rows if float(row.get("elapsed_seconds") or 0) >= 300],
        "average_rows_per_second": round(
            sum(float(row.get("rows_per_second") or 0) for row in rows) / max(1, len(rows)),
            3,
        ),
    }


def summarize_rollback(rows: list[dict]) -> dict[str, Any]:
    return {
        "total": len(rows),
        "success": sum(1 for row in rows if row.get("status") == "SUCCESS"),
        "failed": sum(1 for row in rows if row.get("status") == "FAILED"),
    }


def _split_lob_field(rows: list[dict], field: str) -> list[str]:
    values: list[str] = []
    for row in rows:
        values.extend(item for item in str(row.get(field) or "").split(";") if item)
    return values


def _split_map_values(rows: list[dict], field: str) -> set[str]:
    values: set[str] = set()
    for item in _split_lob_field(rows, field):
        if ":" in item:
            values.add(item.split(":", 1)[1])
    return values
