import unittest

from oracle_pg_sync.config import AppConfig, LobStrategyConfig, OracleConfig, PostgresConfig, TableConfig
from oracle_pg_sync.lob import apply_lob_mapping_policy
from oracle_pg_sync.metadata.type_mapping import ColumnMeta


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


if __name__ == "__main__":
    unittest.main()
