import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oracle_pg_sync.checkpoint import CheckpointStore, Chunk
from oracle_pg_sync.config import AppConfig, IncrementalConfig, OracleConfig, PostgresConfig, TableConfig
from oracle_pg_sync.db import oracle
from oracle_pg_sync.metadata.type_mapping import ColumnMeta
from oracle_pg_sync.sync.postgres_to_oracle import (
    PostgresToOracleSync,
    ReverseSyncResult,
    _apply_checksum_summary,
    _clean_value,
)


class PostgresToOracleSyncTest(unittest.TestCase):
    def test_clean_value_preserves_blank_text_as_space_for_oracle(self):
        self.assertEqual(_clean_value(""), " ")
        self.assertEqual(_clean_value("   "), " ")
        self.assertEqual(_clean_value("\x00"), " ")
        self.assertEqual(_clean_value("ABC"), "ABC")

    def test_reverse_incremental_where_uses_watermark(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
            store.set_watermark(
                direction="postgres_to_oracle",
                table_name="public.sample",
                strategy="updated_at",
                column_name="updated_at",
                value="2026-01-01T00:00:00",
            )
            sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))

            where = sync._incremental_where(
                store,
                TableConfig(
                    name="public.sample",
                    incremental=IncrementalConfig(enabled=True, strategy="updated_at", column="updated_at", overlap_minutes=5),
                ),
                "public.sample",
                incremental=True,
                full_refresh=False,
            )

        self.assertIn('"updated_at" >= TIMESTAMP', where)
        self.assertIn("INTERVAL '5 minutes'", where)

    def test_reverse_incremental_where_supports_restricted_coalesce_expression(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
            store.set_watermark(
                direction="postgres_to_oracle",
                table_name="public.sample",
                strategy="updated_at",
                column_name="coalesce(updated_date,created_date)",
                value="2026-01-01T00:00:00",
            )
            sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
            table_cfg = TableConfig(
                name="public.sample",
                incremental=IncrementalConfig(
                    enabled=True,
                    strategy="updated_at",
                    column="coalesce(updated_date, created_date)",
                    overlap_minutes=5,
                ),
            )
            columns = [
                ColumnMeta(name="updated_date", data_type="timestamp without time zone", ordinal=1),
                ColumnMeta(name="created_date", data_type="timestamp without time zone", ordinal=2),
            ]

            source = sync._resolve_incremental_source(
                table_cfg,
                "public.sample",
                columns,
                incremental=True,
                full_refresh=False,
            )
            where = sync._incremental_where(
                store,
                table_cfg,
                "public.sample",
                incremental_source=source,
                incremental=True,
                full_refresh=False,
            )

        self.assertEqual(source.expression, 'COALESCE("updated_date", "created_date")')
        self.assertEqual(source.columns, ("updated_date", "created_date"))
        self.assertIn('COALESCE("updated_date", "created_date") >= TIMESTAMP', where)

    def test_reverse_incremental_expression_rejects_unassessed_sql(self):
        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        table_cfg = TableConfig(
            name="public.sample",
            incremental=IncrementalConfig(enabled=True, strategy="updated_at", column="updated_at + interval '1 day'"),
        )

        with self.assertRaisesRegex(ValueError, "Unsupported incremental column expression"):
            sync._resolve_incremental_source(
                table_cfg,
                "public.sample",
                [ColumnMeta(name="updated_at", data_type="timestamp without time zone", ordinal=1)],
                incremental=True,
                full_refresh=False,
            )

    def test_reverse_incremental_expression_requires_existing_columns(self):
        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        table_cfg = TableConfig(
            name="public.sample",
            incremental=IncrementalConfig(enabled=True, strategy="updated_at", column="coalesce(updated_at,created_at)"),
        )

        with self.assertRaisesRegex(ValueError, "missing PostgreSQL column"):
            sync._resolve_incremental_source(
                table_cfg,
                "public.sample",
                [ColumnMeta(name="updated_at", data_type="timestamp without time zone", ordinal=1)],
                incremental=True,
                full_refresh=False,
            )

    def test_reverse_checksum_summary_marks_mismatch(self):
        result = type(
            "Result",
            (),
            {
                "checksum_status": "",
                "checksum_source_rows": None,
                "checksum_target_rows": None,
                "checksum_source_hash": "",
                "checksum_target_hash": "",
            },
        )()

        _apply_checksum_summary(
            result,
            [
                {
                    "status": "MISMATCH",
                    "row_count_source": 2,
                    "row_count_target": 1,
                    "source_hash": "a",
                    "target_hash": "b",
                }
            ],
        )

        self.assertEqual(result.checksum_status, "MISMATCH")
        self.assertEqual(result.checksum_source_rows, 2)
        self.assertEqual(result.checksum_target_hash, "b")

    def test_reverse_data_integrity_requires_copy_and_rowcount_match(self):
        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        result = ReverseSyncResult("public.sample", "truncate", "PENDING")
        result.rows_read_from_postgres = 3
        result.rows_written_to_oracle = 3
        result.oracle_row_count = 3
        result.postgres_row_count = 3
        result.row_count_match = True

        self.assertEqual(sync._data_integrity_status(result), "PASS")

        result.oracle_row_count = 2
        result.row_count_match = False
        self.assertEqual(sync._data_integrity_status(result), "FAIL")

    def test_reverse_data_integrity_unknown_when_rowcount_skipped(self):
        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        result = ReverseSyncResult("public.sample", "truncate", "PENDING")
        result.rows_read_from_postgres = 3
        result.rows_written_to_oracle = 3

        self.assertEqual(sync._data_integrity_status(result), "UNKNOWN")

    def test_oracle_merge_rows_uses_merge_and_bind_rows(self):
        class Cursor:
            def __init__(self):
                self.statement = ""
                self.rows = []

            def execute(self, query, params=None):
                if "ALL_TABLES" in query:
                    self._fetchone = ("SAMPLE",)

            def fetchone(self):
                return getattr(self, "_fetchone", None)

            def executemany(self, statement, rows):
                self.statement = statement
                self.rows = rows

        cur = Cursor()

        count = oracle.merge_rows(
            cur,
            owner="APP",
            table="SAMPLE",
            oracle_columns=["ID", "NAME"],
            key_columns=["ID"],
            rows=[(1, "Alice")],
        )

        self.assertEqual(count, 1)
        self.assertIn("MERGE INTO", cur.statement)
        self.assertIn('WHEN MATCHED THEN UPDATE SET t."NAME" = s."NAME"', cur.statement)
        self.assertEqual(cur.rows, [(1, "Alice")])

    def test_reverse_copy_streams_batches_without_fetchall(self):
        class RowsCursor:
            def __init__(self):
                self.rows = [(1, "Alice"), (2, "Bob"), (3, "Cia")]
                self.fetch_sizes = []

            def fetchmany(self, size):
                self.fetch_sizes.append(size)
                batch, self.rows = self.rows[:size], self.rows[size:]
                return batch

            def fetchall(self):
                raise AssertionError("reverse copy must not use fetchall")

        rows_cursor = RowsCursor()
        sync = PostgresToOracleSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
            )
        )
        sync.config.sync.batch_size = 2
        inserted_batches = []

        with patch("oracle_pg_sync.sync.postgres_to_oracle.postgres.select_rows", return_value=rows_cursor), patch(
            "oracle_pg_sync.sync.postgres_to_oracle.oracle.insert_rows",
            side_effect=lambda *args, **kwargs: inserted_batches.append(kwargs["rows"]) or len(kwargs["rows"]),
        ):
            count = sync._copy_pg_to_oracle(
                object(),
                object(),
                "public",
                "sample",
                "APP",
                [("id", "id"), ("name", "name")],
                None,
            )

        self.assertEqual(count, 3)
        self.assertEqual(rows_cursor.fetch_sizes, [2, 2, 2])
        self.assertEqual(inserted_batches, [[(1, "Alice"), (2, "Bob")], [(3, "Cia")]])

    def test_reverse_upsert_uses_oracle_merge(self):
        class RowsCursor:
            def __init__(self):
                self.rows = [[(1, "Alice")], []]

            def fetchmany(self, size):
                return self.rows.pop(0)

        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        merge_calls = []

        with patch("oracle_pg_sync.sync.postgres_to_oracle.postgres.select_rows", return_value=RowsCursor()), patch(
            "oracle_pg_sync.sync.postgres_to_oracle.oracle.merge_rows",
            side_effect=lambda *args, **kwargs: merge_calls.append(kwargs) or len(kwargs["rows"]),
        ):
            count = sync._sync_upsert(
                object(),
                object(),
                "public",
                "sample",
                "APP",
                [("id", "id"), ("name", "name")],
                ["ID"],
                None,
            )

        self.assertEqual(count, 1)
        self.assertEqual(merge_calls[0]["oracle_columns"], ["id", "name"])
        self.assertEqual(merge_calls[0]["key_columns"], ["id"])
        self.assertEqual(merge_calls[0]["rows"], [(1, "Alice")])

    def test_reverse_truncate_truncates_before_insert(self):
        class RowsCursor:
            def __init__(self):
                self.rows = [[(1, "Alice")], []]

            def fetchmany(self, size):
                return self.rows.pop(0)

        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        calls = []

        with patch(
            "oracle_pg_sync.sync.postgres_to_oracle.oracle.truncate_table",
            side_effect=lambda *args, **kwargs: calls.append("truncate"),
        ), patch("oracle_pg_sync.sync.postgres_to_oracle.postgres.select_rows", return_value=RowsCursor()), patch(
            "oracle_pg_sync.sync.postgres_to_oracle.oracle.insert_rows",
            side_effect=lambda *args, **kwargs: calls.append("insert") or len(kwargs["rows"]),
        ):
            count = sync._sync_truncate(
                object(),
                object(),
                "public",
                "sample",
                "APP",
                [("id", "id"), ("name", "name")],
                None,
            )

        self.assertEqual(count, 1)
        self.assertEqual(calls, ["truncate", "insert"])

    def test_reverse_delete_deletes_before_insert(self):
        class RowsCursor:
            def __init__(self):
                self.rows = [[(1, "Alice")], []]

            def fetchmany(self, size):
                return self.rows.pop(0)

        sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))
        calls = []

        with patch(
            "oracle_pg_sync.sync.postgres_to_oracle.oracle.delete_rows",
            side_effect=lambda *args, **kwargs: calls.append("delete"),
        ), patch("oracle_pg_sync.sync.postgres_to_oracle.postgres.select_rows", return_value=RowsCursor()), patch(
            "oracle_pg_sync.sync.postgres_to_oracle.oracle.insert_rows",
            side_effect=lambda *args, **kwargs: calls.append("insert") or len(kwargs["rows"]),
        ):
            count = sync._sync_delete(
                object(),
                object(),
                "public",
                "sample",
                "APP",
                [("id", "id"), ("name", "name")],
                None,
            )

        self.assertEqual(count, 1)
        self.assertEqual(calls, ["delete", "insert"])

    def test_resume_skips_successful_reverse_chunk(self):
        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return self

            def commit(self):
                return None

            def rollback(self):
                return None

        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
            store.create_run(run_id="run1", direction="postgres_to_oracle", source_db="public", target_db="APP")
            store.ensure_chunk(
                run_id="run1",
                direction="postgres_to_oracle",
                source_db="public",
                target_db="APP",
                chunk=Chunk(table_name="public.sample", chunk_key="full"),
            )
            store.finish_chunk("run1", "public.sample", "full", status="success", rows_attempted=1, rows_success=1)
            sync = PostgresToOracleSync(AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")))

            with patch("oracle_pg_sync.sync.postgres_to_oracle.oracle.connect", return_value=Conn()), patch(
                "oracle_pg_sync.sync.postgres_to_oracle.postgres.connect",
                return_value=Conn(),
            ), patch(
                "oracle_pg_sync.sync.postgres_to_oracle.fetch_oracle_metadata",
                return_value=type("Meta", (), {"exists": True, "row_count": 1, "columns": [ColumnMeta('ID', 1, 'NUMBER')], "object_counts": {}})(),
            ), patch(
                "oracle_pg_sync.sync.postgres_to_oracle.fetch_pg_metadata",
                return_value=type("Meta", (), {"exists": True, "row_count": 1, "columns": [ColumnMeta('id', 1, 'integer', udt_name='int4')], "object_counts": {}})(),
            ):
                result = sync.sync_table(
                    "public.sample",
                    execute=True,
                    checkpoint_store=store,
                    run_id="run1",
                    resume=True,
                )

        self.assertEqual(result.status, "SKIPPED")
        self.assertIn("checkpoint chunk full sudah success", result.message)


if __name__ == "__main__":
    unittest.main()
