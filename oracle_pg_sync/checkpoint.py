from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
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

    def create_run(self, *, run_id: str, direction: str, source_db: str, target_db: str) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO sync_runs
                (run_id, direction, source_db, target_db, status, started_at)
                VALUES (?, ?, ?, ?, 'running', ?)
                """,
                (run_id, direction, source_db, target_db, utc_now()),
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
            con.execute("DELETE FROM sync_runs WHERE run_id = ?", (run_id,))

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

    def successful_chunks(self, run_id: str, table_name: str) -> set[str]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT chunk_key FROM sync_chunks WHERE run_id = ? AND table_name = ? AND status = 'success'",
                (run_id, table_name),
            ).fetchall()
        return {str(row[0]) for row in rows}

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


def _validate_status(status: str) -> None:
    if status not in STATUSES:
        raise ValueError(f"Invalid checkpoint status: {status}")


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
