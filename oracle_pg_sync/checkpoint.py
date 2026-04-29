from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


STATUSES = {"pending", "running", "success", "failed", "skipped"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass(frozen=True)
class Chunk:
    table_name: str
    chunk_key: str
    chunk_start: Any = None
    chunk_end: Any = None
    primary_key: str | None = None
    where: str | None = None


@dataclass(frozen=True)
class RollbackAction:
    run_id: str
    table_name: str
    direction: str
    action_type: str
    target_schema: str
    target_table: str
    backup_schema: str = ""
    backup_table: str = ""
    staging_schema: str = ""
    staging_table: str = ""
    prior_watermark: str = ""
    dependency_state: dict[str, Any] | None = None
    status: str = "ready"
    notes: str = ""


class CheckpointStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self):
        return sqlite3.connect(self.path)

    def _init_schema(self) -> None:
        with self.connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                    run_id TEXT PRIMARY KEY,
                    direction TEXT NOT NULL,
                    source_db TEXT,
                    target_db TEXT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    error_message TEXT
                )
                """
            )
            self._ensure_column(con, "sync_runs", "job_name", "TEXT")
            self._ensure_column(con, "sync_runs", "mode", "TEXT")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_chunks (
                    run_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    source_db TEXT,
                    target_db TEXT,
                    table_name TEXT NOT NULL,
                    primary_key TEXT,
                    chunk_key TEXT NOT NULL,
                    chunk_start TEXT,
                    chunk_end TEXT,
                    rows_attempted INTEGER DEFAULT 0,
                    rows_success INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    PRIMARY KEY (run_id, table_name, chunk_key)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS rollback_actions (
                    run_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    target_schema TEXT NOT NULL,
                    target_table TEXT NOT NULL,
                    backup_schema TEXT,
                    backup_table TEXT,
                    staging_schema TEXT,
                    staging_table TEXT,
                    prior_watermark TEXT,
                    dependency_state TEXT,
                    status TEXT NOT NULL,
                    notes TEXT,
                    created_at TEXT NOT NULL,
                    restored_at TEXT,
                    PRIMARY KEY (run_id, table_name, action_type)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS run_events (
                    run_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    table_name TEXT,
                    phase TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    details TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS circuit_breakers (
                    job_key TEXT PRIMARY KEY,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_failure_at TEXT,
                    cooldown_until TEXT,
                    last_error TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS watermarks (
                    direction TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    watermark_value TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (direction, table_name, strategy, column_name)
                )
                """
            )

    def create_run(
        self,
        *,
        run_id: str,
        direction: str,
        source_db: str,
        target_db: str,
        job_name: str = "",
        mode: str = "",
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO sync_runs
                (run_id, direction, source_db, target_db, status, started_at, job_name, mode)
                VALUES (?, ?, ?, ?, 'running', ?, ?, ?)
                """,
                (run_id, direction, source_db, target_db, utc_now(), job_name, mode),
            )

    def finish_run(self, run_id: str, *, status: str, error_message: str = "") -> None:
        _validate_status(status)
        with self.connect() as con:
            con.execute(
                """
                UPDATE sync_runs
                SET status = ?, finished_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                (status, utc_now(), error_message, run_id),
            )

    def list_runs(self) -> list[dict[str, Any]]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            return [dict(row) for row in con.execute("SELECT * FROM sync_runs ORDER BY started_at DESC")]

    def reset_run(self, run_id: str) -> None:
        with self.connect() as con:
            con.execute("DELETE FROM sync_chunks WHERE run_id = ?", (run_id,))
            con.execute("DELETE FROM rollback_actions WHERE run_id = ?", (run_id,))
            con.execute("DELETE FROM run_events WHERE run_id = ?", (run_id,))
            con.execute("DELETE FROM sync_runs WHERE run_id = ?", (run_id,))

    def record_event(
        self,
        *,
        run_id: str,
        phase: str,
        status: str,
        table_name: str = "",
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT INTO run_events
                (run_id, event_time, table_name, phase, status, message, details)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, utc_now(), table_name, phase, status, message, _json_text(details)),
            )

    def list_events(self, run_id: str) -> list[dict[str, Any]]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT * FROM run_events WHERE run_id = ? ORDER BY event_time, table_name, phase",
                (run_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = _json_load(item.get("details"))
            result.append(item)
        return result

    def add_rollback_action(self, action: RollbackAction) -> None:
        payload = asdict(action)
        with self.connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO rollback_actions (
                    run_id, table_name, direction, action_type, target_schema, target_table,
                    backup_schema, backup_table, staging_schema, staging_table,
                    prior_watermark, dependency_state, status, notes, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["run_id"],
                    payload["table_name"],
                    payload["direction"],
                    payload["action_type"],
                    payload["target_schema"],
                    payload["target_table"],
                    payload["backup_schema"],
                    payload["backup_table"],
                    payload["staging_schema"],
                    payload["staging_table"],
                    payload["prior_watermark"],
                    _json_text(payload["dependency_state"]),
                    payload["status"],
                    payload["notes"],
                    utc_now(),
                ),
            )

    def rollback_actions(self, run_id: str) -> list[dict[str, Any]]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT * FROM rollback_actions WHERE run_id = ? ORDER BY created_at DESC, table_name",
                (run_id,),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["dependency_state"] = _json_load(item.get("dependency_state"))
            result.append(item)
        return result

    def mark_rollback_action(self, run_id: str, table_name: str, action_type: str, *, status: str, notes: str = "") -> None:
        with self.connect() as con:
            con.execute(
                """
                UPDATE rollback_actions
                SET status = ?, notes = ?, restored_at = ?
                WHERE run_id = ? AND table_name = ? AND action_type = ?
                """,
                (status, notes, utc_now(), run_id, table_name, action_type),
            )

    def circuit_status(self, job_key: str) -> dict[str, Any] | None:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            row = con.execute("SELECT * FROM circuit_breakers WHERE job_key = ?", (job_key,)).fetchone()
        return dict(row) if row else None

    def register_job_failure(self, job_key: str, *, cooldown_minutes: int, error_message: str) -> dict[str, Any]:
        current = self.circuit_status(job_key) or {}
        failure_count = int(current.get("failure_count") or 0) + 1
        now = utc_now()
        cooldown_until = ""
        if cooldown_minutes > 0:
            cooldown_until = (
                datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=int(cooldown_minutes))
            ).isoformat(timespec="seconds")
        with self.connect() as con:
            con.execute(
                """
                INSERT INTO circuit_breakers (job_key, failure_count, last_failure_at, cooldown_until, last_error)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_key) DO UPDATE SET
                    failure_count = excluded.failure_count,
                    last_failure_at = excluded.last_failure_at,
                    cooldown_until = excluded.cooldown_until,
                    last_error = excluded.last_error
                """,
                (job_key, failure_count, now, cooldown_until, error_message),
            )
        return self.circuit_status(job_key) or {}

    def clear_job_failures(self, job_key: str) -> None:
        with self.connect() as con:
            con.execute("DELETE FROM circuit_breakers WHERE job_key = ?", (job_key,))

    def job_blocked(self, job_key: str, *, max_failures: int) -> dict[str, Any] | None:
        status = self.circuit_status(job_key)
        if not status:
            return None
        if int(status.get("failure_count") or 0) < int(max_failures or 0):
            return None
        cooldown_until = _parse_dt(status.get("cooldown_until"))
        if cooldown_until and cooldown_until > datetime.now(timezone.utc):
            return status
        return None

    def ensure_chunk(
        self,
        *,
        run_id: str,
        direction: str,
        source_db: str,
        target_db: str,
        chunk: Chunk,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO sync_chunks (
                    run_id, direction, source_db, target_db, table_name, primary_key,
                    chunk_key, chunk_start, chunk_end, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    run_id,
                    direction,
                    source_db,
                    target_db,
                    chunk.table_name,
                    chunk.primary_key,
                    chunk.chunk_key,
                    _to_text(chunk.chunk_start),
                    _to_text(chunk.chunk_end),
                ),
            )

    def chunk_status(self, run_id: str, table_name: str, chunk_key: str) -> str | None:
        with self.connect() as con:
            row = con.execute(
                "SELECT status FROM sync_chunks WHERE run_id = ? AND table_name = ? AND chunk_key = ?",
                (run_id, table_name, chunk_key),
            ).fetchone()
        return str(row[0]) if row else None

    def start_chunk(self, run_id: str, table_name: str, chunk_key: str) -> None:
        with self.connect() as con:
            con.execute(
                """
                UPDATE sync_chunks
                SET status = 'running', retry_count = retry_count + 1, started_at = ?, error_message = NULL
                WHERE run_id = ? AND table_name = ? AND chunk_key = ?
                """,
                (utc_now(), run_id, table_name, chunk_key),
            )

    def finish_chunk(
        self,
        run_id: str,
        table_name: str,
        chunk_key: str,
        *,
        status: str,
        rows_attempted: int = 0,
        rows_success: int = 0,
        error_message: str = "",
    ) -> None:
        _validate_status(status)
        with self.connect() as con:
            con.execute(
                """
                UPDATE sync_chunks
                SET status = ?, rows_attempted = ?, rows_success = ?, error_message = ?, finished_at = ?
                WHERE run_id = ? AND table_name = ? AND chunk_key = ?
                """,
                (status, rows_attempted, rows_success, error_message, utc_now(), run_id, table_name, chunk_key),
            )

    def mark_table_phase(
        self,
        *,
        run_id: str,
        direction: str,
        source_db: str,
        target_db: str,
        table_name: str,
        phase: str,
        status: str = "success",
        rows_attempted: int = 0,
        rows_success: int = 0,
        error_message: str = "",
    ) -> None:
        chunk = Chunk(table_name=table_name, chunk_key=phase)
        self.ensure_chunk(
            run_id=run_id,
            direction=direction,
            source_db=source_db,
            target_db=target_db,
            chunk=chunk,
        )
        self.start_chunk(run_id, table_name, phase)
        self.finish_chunk(
            run_id,
            table_name,
            phase,
            status=status,
            rows_attempted=rows_attempted,
            rows_success=rows_success,
            error_message=error_message,
        )

    def successful_chunks(self, run_id: str, table_name: str) -> set[str]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT chunk_key FROM sync_chunks WHERE run_id = ? AND table_name = ? AND status = 'success'",
                (run_id, table_name),
            ).fetchall()
        return {str(row[0]) for row in rows}

    def list_chunks(self, run_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM sync_chunks"
        params: tuple[Any, ...] = ()
        if run_id:
            query += " WHERE run_id = ?"
            params = (run_id,)
        query += " ORDER BY table_name, chunk_key"
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            return [dict(row) for row in con.execute(query, params)]

    def get_watermark(self, *, direction: str, table_name: str, strategy: str, column_name: str) -> str | None:
        with self.connect() as con:
            row = con.execute(
                """
                SELECT watermark_value FROM watermarks
                WHERE direction = ? AND table_name = ? AND strategy = ? AND column_name = ?
                """,
                (direction, table_name, strategy, column_name),
            ).fetchone()
        return str(row[0]) if row and row[0] is not None else None

    def set_watermark(
        self,
        *,
        direction: str,
        table_name: str,
        strategy: str,
        column_name: str,
        value: Any,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT INTO watermarks
                (direction, table_name, strategy, column_name, watermark_value, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(direction, table_name, strategy, column_name)
                DO UPDATE SET watermark_value = excluded.watermark_value, updated_at = excluded.updated_at
                """,
                (direction, table_name, strategy, column_name, _to_text(value), utc_now()),
            )

    def list_watermarks(self) -> list[dict[str, Any]]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            return [dict(row) for row in con.execute("SELECT * FROM watermarks ORDER BY table_name, column_name")]

    def reset_watermark(self, table_name: str) -> int:
        with self.connect() as con:
            cur = con.execute("DELETE FROM watermarks WHERE table_name = ?", (table_name,))
            return int(cur.rowcount or 0)

    @staticmethod
    def _ensure_column(con: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
        existing = {
            str(row[1]).lower()
            for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name.lower() not in existing:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _validate_status(status: str) -> None:
    if status not in STATUSES:
        raise ValueError(f"Invalid checkpoint status: {status}")


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _json_text(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value or {}, sort_keys=True)


def _json_load(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None
