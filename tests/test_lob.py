import unittest
import sys
import types

if "psycopg" not in sys.modules:
    psycopg_stub = types.ModuleType("psycopg")
    psycopg_stub.sql = types.SimpleNamespace(
        SQL=lambda value: value,
        Identifier=lambda value: value,
    )
    sys.modules["psycopg"] = psycopg_stub

from oracle_pg_sync.config import AppConfig, LobStrategyConfig, OracleConfig, PostgresConfig, TableConfig
from oracle_pg_sync.lob import (
    apply_lob_mapping_policy,
    is_lob_column,
    oracle_lob_validation_expressions,
    postgres_lob_validation_expressions,
    target_type_for_lob,
)
from oracle_pg_sync.metadata.type_mapping import ColumnMeta
from oracle_pg_sync.sync.copy_loader import _sanitize_value


class LobStrategyTest(unittest.TestCase):
    def test_default_error_fails_for_lob(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            lob_strategy=LobStrategyConfig(default="error"),
        )

        with self.assertRaises(ValueError):
            apply_lob_mapping_policy(
                [("blob_payload", "BLOB_PAYLOAD")],
                config=config,
                table_cfg=TableConfig(name="public.sample_blob_table"),
                table_name="public.sample_blob_table",
                source_columns=[ColumnMeta("BLOB_PAYLOAD", 1, "BLOB")],
            )

    def test_null_strategy_keeps_column_with_null_source(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            lob_strategy=LobStrategyConfig(default="error"),
        )
        mapping, summary = apply_lob_mapping_policy(
            [("blob_payload", "BLOB_PAYLOAD"), ("id", "ID")],
            config=config,
            table_cfg=TableConfig(
                name="public.sample_blob_table",
                lob_strategy=LobStrategyConfig(columns={"BLOB_PAYLOAD": "null"}),
            ),
            table_name="public.sample_blob_table",
            source_columns=[ColumnMeta("BLOB_PAYLOAD", 1, "BLOB"), ColumnMeta("ID", 2, "NUMBER")],
        )

        self.assertEqual(mapping, [("blob_payload", None), ("id", "ID")])
        self.assertEqual(summary["lob_columns_nullified"], ["blob_payload"])

    def test_skip_strategy_removes_lob_column(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            lob_strategy=LobStrategyConfig(default="skip"),
        )
        mapping, summary = apply_lob_mapping_policy(
            [("blob_payload", "BLOB_PAYLOAD"), ("id", "ID")],
            config=config,
            table_cfg=TableConfig(name="public.sample_blob_table"),
            table_name="public.sample_blob_table",
            source_columns=[ColumnMeta("BLOB_PAYLOAD", 1, "BLOB"), ColumnMeta("ID", 2, "NUMBER")],
        )

        self.assertEqual(mapping, [("id", "ID")])
        self.assertEqual(summary["lob_columns_skipped"], ["blob_payload"])

    def test_all_oracle_lob_types_are_detected_and_mapped(self):
        cases = [
            ("BLOB", "bytea"),
            ("CLOB", "text"),
            ("NCLOB", "text"),
            ("LONG", "text"),
            ("LONG RAW", "bytea"),
        ]

        for data_type, target_type in cases:
            with self.subTest(data_type=data_type):
                column = ColumnMeta("PAYLOAD", 1, data_type)
                self.assertTrue(is_lob_column(column))
                self.assertEqual(target_type_for_lob(column), target_type)

    def test_stream_strategy_supports_rich_column_config(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            lob_strategy=LobStrategyConfig(default="error"),
        )
        mapping, summary = apply_lob_mapping_policy(
            [("message", "MESSAGE")],
            config=config,
            table_cfg=TableConfig(
                name="public.logs",
                lob_strategy=LobStrategyConfig(columns={"MESSAGE": {"strategy": "stream", "target_type": "text", "validation": "size_hash"}}),
            ),
            table_name="public.logs",
            source_columns=[ColumnMeta("MESSAGE", 1, "NCLOB")],
        )

        self.assertEqual(mapping, [("message", "MESSAGE")])
        self.assertEqual(summary["lob_columns_synced"], ["message"])
        self.assertEqual(summary["lob_type"]["message"], "NCLOB")
        self.assertEqual(summary["lob_target_type"]["message"], "text")
        self.assertEqual(summary["lob_validation_mode"]["message"], "size_hash")

    def test_reverse_policy_uses_oracle_target_lob_metadata(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            lob_strategy=LobStrategyConfig(default="error"),
        )

        with self.assertRaises(ValueError):
            apply_lob_mapping_policy(
                [("message", "message")],
                config=config,
                table_cfg=TableConfig(name="public.logs"),
                table_name="public.logs",
                source_columns=[ColumnMeta("MESSAGE", 1, "CLOB")],
                policy_column_side="target",
            )

    def test_lob_validation_expressions_do_not_include_raw_values(self):
        blob = ColumnMeta("HOUSE_IMAGE", 1, "BLOB")
        clob = ColumnMeta("JSON_DATA", 2, "CLOB")
        long_col = ColumnMeta("MESSAGE", 3, "LONG")

        self.assertIn("DBMS_LOB.GETLENGTH", oracle_lob_validation_expressions("HOUSE_IMAGE", blob)["size"])
        self.assertIn("DBMS_CRYPTO.HASH", oracle_lob_validation_expressions("HOUSE_IMAGE", blob)["hash"])
        self.assertIn("octet_length", postgres_lob_validation_expressions("house_image", ColumnMeta("house_image", 1, "bytea", udt_name="bytea"))["size"])
        self.assertIn("skipped_with_reason", oracle_lob_validation_expressions("JSON_DATA", clob)["hash_validation_status"])
        self.assertIn("skipped_with_reason", oracle_lob_validation_expressions("MESSAGE", long_col)["hash_validation_status"])

    def test_lob_reader_uses_chunked_read_when_available(self):
        class FakeBlob:
            def __init__(self):
                self.size = 6
                self.calls = []

            def read(self, offset, amount):
                self.calls.append((offset, amount))
                data = b"abcdef"
                return data[offset - 1 : offset - 1 + amount]

        blob = FakeBlob()

        self.assertEqual(_sanitize_value(blob, lob_chunk_size_bytes=2), b"abcdef")
        self.assertEqual(blob.calls, [(1, 2), (3, 2), (5, 2), (7, 2)])


if __name__ == "__main__":
    unittest.main()
