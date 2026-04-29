"""
Optional integration check for the production-safe PostgreSQL sync paths.

Run manually:

    RUN_CONTAINER_TESTS=1 python3 tests/integration_safe_sync_container.py

The test uses a real PostgreSQL container (or PGHOST/PGPORT/... from the
environment) and a mocked Oracle layer so safe cutover, rollback, dependency
repair, reverse MERGE, and LOB streaming can be exercised without Oracle.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if os.getenv("RUN_CONTAINER_TESTS") != "1":
    print("Set RUN_CONTAINER_TESTS=1 to run the PostgreSQL-backed safe sync integration check.")
    raise SystemExit(0)

from oracle_pg_sync.checkpoint import CheckpointStore
from oracle_pg_sync.cli import _run_dependency_maintenance
from oracle_pg_sync.config import (
    AppConfig,
    ChecksumConfig,
    LobStrategyConfig,
    OracleConfig,
    PostgresConfig,
    SyncConfig,
    TableConfig,
    ValidationConfig,
)
from oracle_pg_sync.metadata.oracle_metadata import OracleTableMetadata
from oracle_pg_sync.metadata.type_mapping import ColumnMeta
from oracle_pg_sync.rollback import rollback_run
from oracle_pg_sync.sync.oracle_to_postgres import OracleToPostgresSync


def main() -> int:
    if os.getenv("PGHOST"):
        conn = _pg_conn_from_env()
        _wait_for_postgres(**conn)
        _run_all(conn)
        print("integration safe sync probe OK")
        return 0

    name = f"oracle-pg-safe-it-{uuid.uuid4().hex[:8]}"
    conn = {
        "host": "127.0.0.1",
        "port": _free_tcp_port(),
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
    }
    try:
        subprocess.check_call(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                name,
                "-e",
                f"POSTGRES_PASSWORD={conn['password']}",
                "-p",
                f"{conn['port']}:5432",
                "postgres:16-alpine",
            ]
        )
        _wait_for_postgres(**conn)
        _run_all(conn)
        print("integration safe sync probe OK")
        return 0
    finally:
        subprocess.call(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _run_all(conn: dict) -> None:
    import psycopg

    from tests.integration_reverse_merge_container import _run_reverse_merge_probe

    with tempfile.TemporaryDirectory() as tmp:
        checkpoint = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(
                host=conn["host"],
                port=conn["port"],
                database=conn["dbname"],
                user=conn["user"],
                password=conn["password"],
                schema="public",
            ),
            sync=SyncConfig(
                dry_run=False,
                default_mode="truncate_safe",
                exact_count_after_load=False,
                backup_before_truncate=True,
            ),
            validation=ValidationConfig(checksum=ChecksumConfig(enabled=True)),
            tables=[
                TableConfig(
                    name="public.safe_sample",
                    key_columns=["id"],
                ),
                TableConfig(
                    name="public.safe_lob",
                    key_columns=["id"],
                    lob_strategy=LobStrategyConfig(default="stream"),
                ),
            ],
            lob_strategy=LobStrategyConfig(default="stream"),
        )
        with psycopg.connect(**config.postgres.conninfo()) as con:
            with con.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS public.safe_sample")
                cur.execute("CREATE TABLE public.safe_sample (id integer primary key, name text)")
                cur.execute("INSERT INTO public.safe_sample VALUES (99, 'before')")
                cur.execute("DROP TABLE IF EXISTS public.safe_lob")
                cur.execute("CREATE TABLE public.safe_lob (id integer primary key, payload bytea, note text)")
            con.commit()

        _run_truncate_safe(config, checkpoint)
        _run_rollback(config, checkpoint)
        _run_swap_safe(config, checkpoint)
        _run_lob_streaming(config, checkpoint)
        _run_dependency_loop(config)
        _run_reverse_merge_probe(**conn)


def _run_truncate_safe(config: AppConfig, checkpoint: CheckpointStore) -> None:
    rows = [(1, "Alice"), (2, "Bob")]
    result = _run_safe_sync(
        config,
        checkpoint,
        table_name="public.safe_sample",
        mode="truncate_safe",
        rows=rows,
        oracle_columns=[ColumnMeta("ID", 1, "NUMBER"), ColumnMeta("NAME", 2, "VARCHAR2")],
        run_id="truncate123",
    )
    assert result.status == "SUCCESS"
    assert result.rollback_available is True
    import psycopg

    with psycopg.connect(**config.postgres.conninfo()) as con:
        with con.cursor() as cur:
            cur.execute("SELECT id, name FROM public.safe_sample ORDER BY id")
            assert cur.fetchall() == rows


def _run_rollback(config: AppConfig, checkpoint: CheckpointStore) -> None:
    import psycopg

    rows = rollback_run(config, checkpoint, run_id="truncate123")
    assert rows and rows[0]["status"] == "SUCCESS"
    with psycopg.connect(**config.postgres.conninfo()) as con:
        with con.cursor() as cur:
            cur.execute("SELECT id, name FROM public.safe_sample ORDER BY id")
            assert cur.fetchall() == [(99, "before")]


def _run_swap_safe(config: AppConfig, checkpoint: CheckpointStore) -> None:
    rows = [(10, "Swap"), (11, "Safe")]
    result = _run_safe_sync(
        config,
        checkpoint,
        table_name="public.safe_sample",
        mode="swap_safe",
        rows=rows,
        oracle_columns=[ColumnMeta("ID", 1, "NUMBER"), ColumnMeta("NAME", 2, "VARCHAR2")],
        run_id="swap123",
    )
    assert result.status == "SUCCESS"
    assert result.rollback_action == "swap_safe"


def _run_lob_streaming(config: AppConfig, checkpoint: CheckpointStore) -> None:
    class FakeBlob:
        def __init__(self, data: bytes):
            self._data = data
            self.size = len(data)

        def read(self, offset: int, amount: int) -> bytes:
            return self._data[offset - 1 : offset - 1 + amount]

    rows = [(1, FakeBlob(b"hello-bytes"), "hello text")]
    result = _run_safe_sync(
        config,
        checkpoint,
        table_name="public.safe_lob",
        mode="truncate_safe",
        rows=rows,
        oracle_columns=[ColumnMeta("ID", 1, "NUMBER"), ColumnMeta("PAYLOAD", 2, "BLOB"), ColumnMeta("NOTE", 3, "CLOB")],
        run_id="lob123",
    )
    assert result.status == "SUCCESS"
    assert result.lob_bytes_processed > 0


def _run_dependency_loop(config: AppConfig) -> None:
    calls: list[str] = []

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return self

        def commit(self):
            calls.append("commit")

    invalid_states = [
        [{"object_schema": "APP", "object_type": "VIEW", "object_name": "V_BAD", "status": "INVALID"}],
        [],
    ]

    with tempfile.TemporaryDirectory() as tmp:
        with (
            patch("oracle_pg_sync.db.oracle.connect", return_value=Conn()),
            patch("oracle_pg_sync.db.postgres.connect", return_value=Conn()),
            patch("oracle_pg_sync.db.postgres.refresh_materialized_views", side_effect=lambda *args, **kwargs: calls.append("refresh") or []),
            patch("oracle_pg_sync.db.oracle.compile_invalid_objects", side_effect=lambda *args, **kwargs: calls.append("compile") or invalid_states[0]),
            patch("oracle_pg_sync.db.oracle.invalid_object_rows", side_effect=lambda *args, **kwargs: invalid_states.pop(0) if invalid_states else []),
            patch("oracle_pg_sync.db.postgres.validate_dependent_objects", side_effect=lambda *args, **kwargs: calls.append("validate") or []),
        ):
            rows = _run_dependency_maintenance(
                config,
                ["public.safe_sample"],
                __import__("logging").getLogger("integration_dependency"),
                Path(tmp),
                [],
                execute=True,
            )
    assert any(row.get("maintenance_status") == "fixed" for row in rows)
    assert calls[:3] == ["refresh", "compile", "commit"]


def _run_safe_sync(
    config: AppConfig,
    checkpoint: CheckpointStore,
    *,
    table_name: str,
    mode: str,
    rows: list[tuple],
    oracle_columns: list[ColumnMeta],
    run_id: str,
):
    sync = OracleToPostgresSync(config)

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return self

    class RowsCursor:
        def __init__(self, data):
            self.data = list(data)

        def fetchmany(self, size):
            batch, self.data = self.data[:size], self.data[size:]
            return batch

    oracle_meta = OracleTableMetadata(
        exists=True,
        row_count=len(rows),
        columns=oracle_columns,
        object_counts={},
    )
    with (
        patch("oracle_pg_sync.sync.oracle_to_postgres.oracle.connect", return_value=DummyConn()),
        patch("oracle_pg_sync.sync.oracle_to_postgres.fetch_oracle_metadata", return_value=oracle_meta),
        patch("oracle_pg_sync.sync.oracle_to_postgres.compare_table_metadata", return_value=({"status": "MATCH"}, [], [])),
        patch("oracle_pg_sync.sync.oracle_to_postgres.inventory_has_fatal_mismatch", return_value=False),
        patch("oracle_pg_sync.sync.oracle_to_postgres.oracle.select_rows", side_effect=lambda *args, **kwargs: RowsCursor(rows)),
        patch("oracle_pg_sync.sync.oracle_to_postgres.oracle.count_rows_where", side_effect=lambda *args, **kwargs: len(rows)),
        patch("oracle_pg_sync.sync.oracle_to_postgres.oracle.max_value", side_effect=lambda *args, **kwargs: max(row[0] for row in rows)),
        patch("oracle_pg_sync.sync.oracle_to_postgres.oracle.min_max", return_value=(1, len(rows))),
    ):
        result = sync.sync_table(
            table_name,
            mode_override=mode,
            execute=True,
            checkpoint_store=checkpoint,
            run_id=run_id,
            incremental=mode == "incremental_safe",
        )
    return result


def _pg_conn_from_env() -> dict:
    return {
        "host": os.getenv("PGHOST", "127.0.0.1"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "postgres"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "postgres"),
    }


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_postgres(host: str, port: int, dbname: str, user: str, password: str) -> None:
    import psycopg

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError("PostgreSQL did not become ready")


if __name__ == "__main__":
    raise SystemExit(main())
