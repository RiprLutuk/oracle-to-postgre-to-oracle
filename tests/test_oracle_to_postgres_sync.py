import unittest
import sys
import types
import logging
import threading
from unittest.mock import patch

if "psycopg" not in sys.modules:
    psycopg_stub = types.ModuleType("psycopg")
    psycopg_stub.connect = lambda *args, **kwargs: None
    psycopg_stub.sql = types.SimpleNamespace(
        SQL=lambda value: value,
        Identifier=lambda value: value,
        Literal=lambda value: value,
    )
    sys.modules["psycopg"] = psycopg_stub

if "oracledb" not in sys.modules:
    oracledb_stub = types.ModuleType("oracledb")
    oracledb_stub.connect = lambda *args, **kwargs: None
    oracledb_stub.init_oracle_client = lambda *args, **kwargs: None
    oracledb_stub.makedsn = lambda host, port, service_name=None, sid=None: "oracle-dsn"
    sys.modules["oracledb"] = oracledb_stub

from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig, SyncConfig, TableConfig
from oracle_pg_sync.sync.oracle_to_postgres import OracleToPostgresSync, SyncResult
from oracle_pg_sync.sync.runtime import SyncExecutionContext


class OracleToPostgresSyncTest(unittest.TestCase):
    def test_swap_execute_is_guarded_by_default(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(allow_swap=False),
            )
        )

        message = sync._swap_guard_message("public.sample_customer", 1024, force=False)

        self.assertIn("mode swap dinonaktifkan", message)

    def test_swap_max_size_accepts_force_bypass(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(allow_swap=True, max_swap_table_bytes=1024),
            )
        )

        message = sync._swap_guard_message("public.sample_customer", 2048, force=True)

        self.assertEqual(message, "")

    def test_swap_dry_run_mentions_estimated_storage(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(swap_space_multiplier=2.5),
            )
        )

        message = sync._dry_run_message("public.sample_customer", "swap", 3, 1024**3)

        self.assertIn("2.5 GiB", message)

    def test_truncate_resume_never_skips_successful_chunks(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(truncate_resume_strategy="restart_table"),
            ),
            logger=logging.getLogger("test_truncate_resume"),
        )
        sync.logger.disabled = True

        successful = sync._truncate_resume_successful_chunks(
            "public.sample_customer",
            {"id:1:10"},
            resume=True,
        )

        self.assertEqual(successful, set())

    def test_modes_preserve_requested_safety_level(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
            )
        )

        self.assertEqual(sync._normalize_mode("truncate", incremental=False), "truncate")
        self.assertEqual(sync._normalize_mode("truncate_safe", incremental=False), "truncate_safe")
        self.assertEqual(sync._normalize_mode("swap", incremental=False), "swap")
        self.assertEqual(sync._normalize_mode("swap_safe", incremental=False), "swap_safe")
        self.assertEqual(sync._normalize_mode("upsert", incremental=False), "incremental_safe")

    def test_copy_mismatch_fails_completeness_validation(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
            )
        )
        result = types.SimpleNamespace(rows_failed=0, rows_read_from_oracle=10, rows_written_to_postgres=9)

        with self.assertRaises(RuntimeError):
            sync._validate_copy_completeness(result)

    def test_data_integrity_requires_copy_and_rowcount_match(self):
        sync = OracleToPostgresSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        result = SyncResult("public.sample", "truncate", "PENDING")
        result.rows_read_from_oracle = 3
        result.rows_written_to_postgres = 3
        result.rows_failed = 0
        result.oracle_row_count = 3
        result.postgres_row_count = 3
        result.row_count_match = True

        self.assertEqual(sync._data_integrity_status(result), "PASS")

        result.postgres_row_count = 2
        result.row_count_match = False
        self.assertEqual(sync._data_integrity_status(result), "FAIL")

    def test_data_integrity_unknown_when_rowcount_skipped(self):
        sync = OracleToPostgresSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        result = SyncResult("public.sample", "truncate", "PENDING")
        result.rows_read_from_oracle = 3
        result.rows_written_to_postgres = 3

        self.assertEqual(sync._data_integrity_status(result), "UNKNOWN")

    def test_skip_if_rowcount_match_precheck_requires_explicit_flag_and_full_refresh_shape(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(skip_if_rowcount_match=True),
            )
        )
        table_cfg = TableConfig(name="public.sample")

        self.assertTrue(
            sync._should_skip_if_rowcount_match(
                table_cfg,
                "truncate_safe",
                None,
                None,
                full_refresh=False,
            )
        )
        self.assertFalse(
            sync._should_skip_if_rowcount_match(
                table_cfg,
                "append",
                None,
                None,
                full_refresh=False,
            )
        )
        self.assertFalse(
            sync._should_skip_if_rowcount_match(
                table_cfg,
                "truncate_safe",
                "status = 'A'",
                None,
                full_refresh=False,
            )
        )
        table_cfg.incremental.enabled = True
        self.assertFalse(
            sync._should_skip_if_rowcount_match(
                table_cfg,
                "truncate_safe",
                None,
                None,
                full_refresh=False,
            )
        )
        self.assertTrue(
            sync._should_skip_if_rowcount_match(
                table_cfg,
                "truncate_safe",
                None,
                None,
                full_refresh=True,
            )
        )

    def test_sync_tables_parallel_executes_all_tables(self):
        class FakeExecutionContext:
            def __init__(self):
                self.workers = 2
                self.max_db_connections = 2
                self._labels = {}

            def allow_table_parallelism(self, table_count):
                return table_count > 1

            def allow_chunk_parallelism(self, *, mode, table_count, chunk_count):
                return False

            def close(self):
                return None

            def worker_label(self):
                ident = threading.get_ident()
                if ident not in self._labels:
                    self._labels[ident] = f"Worker-{len(self._labels) + 1}"
                return self._labels[ident]

            def table_logger(self, logger, table_name):
                return logger

        def fake_sync_table(
            self,
            table_name,
            *,
            execution_context=None,
            **kwargs,
        ):
            return types.SimpleNamespace(
                table_name=table_name,
                status="SUCCESS",
                worker_name=execution_context.worker_label() if execution_context else "",
            )

        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(workers=2, parallel_tables=True, max_db_connections=2),
            )
        )

        with patch("oracle_pg_sync.sync.oracle_to_postgres.create_sync_execution_context", return_value=FakeExecutionContext()), patch.object(
            OracleToPostgresSync,
            "sync_table",
            new=fake_sync_table,
        ):
            results = sync.sync_tables(["public.a", "public.b"], execute=True)

        self.assertEqual([result.table_name for result in results], ["public.a", "public.b"])
        self.assertTrue(all(result.status == "SUCCESS" for result in results))

    def test_execution_context_reuses_oracle_connection_per_worker(self):
        class DummyConnection:
            def close(self):
                return None

        class DummyPool:
            def connection(self):
                class _Handle:
                    def __enter__(self_inner):
                        return DummyConnection()

                    def __exit__(self_inner, *args):
                        return False

                return _Handle()

            def close(self):
                return None

        connect_calls = []
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(host="localhost", database="db", user="user", password="secret", schema="public"),
            sync=SyncConfig(workers=2, parallel_tables=True, max_db_connections=2),
        )
        logger = logging.getLogger("test_execution_context_reuses_oracle_connection_per_worker")

        with patch("oracle_pg_sync.sync.runtime.postgres.connection_pool", return_value=DummyPool()), patch(
            "oracle_pg_sync.sync.runtime.oracle.connect",
            side_effect=lambda cfg: connect_calls.append(object()) or DummyConnection(),
        ):
            context = SyncExecutionContext(config, logger)
            try:
                with context.oracle_connection() as first:
                    with context.oracle_connection() as second:
                        self.assertIs(first, second)
            finally:
                context.close()

        self.assertEqual(len(connect_calls), 1)


if __name__ == "__main__":
    unittest.main()
