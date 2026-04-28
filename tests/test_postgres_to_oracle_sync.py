import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oracle_pg_sync.checkpoint import CheckpointStore
from oracle_pg_sync.config import AppConfig, IncrementalConfig, OracleConfig, PostgresConfig, TableConfig
from oracle_pg_sync.db import oracle
from oracle_pg_sync.sync.postgres_to_oracle import PostgresToOracleSync, _apply_checksum_summary


class PostgresToOracleSyncTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
