import unittest

from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig
from oracle_pg_sync.metadata.compare import compare_table_metadata
from oracle_pg_sync.metadata.oracle_metadata import OracleTableMetadata
from oracle_pg_sync.metadata.postgres_metadata import PostgresTableMetadata
from oracle_pg_sync.metadata.type_mapping import ColumnMeta


class CompareTest(unittest.TestCase):
    def test_rename_mapping_counts_as_same_column(self):
        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            rename_columns={"public.sample": {"freeze": "freezee"}},
        )
        oracle_meta = OracleTableMetadata(
            exists=True,
            row_count=10,
            columns=[
                ColumnMeta("ID", 1, "NUMBER", numeric_precision=9, numeric_scale=0),
                ColumnMeta("FREEZE", 2, "VARCHAR2", char_length=10),
            ],
            object_counts={},
        )
        pg_meta = PostgresTableMetadata(
            exists=True,
            row_count=10,
            columns=[
                ColumnMeta("id", 1, "integer", udt_name="int4"),
                ColumnMeta("freezee", 2, "varchar", char_length=10, udt_name="varchar"),
            ],
            object_counts={},
        )

        inventory, column_diff, type_mismatch = compare_table_metadata(
            table_name="public.sample",
            config=config,
            oracle_meta=oracle_meta,
            postgres_meta=pg_meta,
        )

        self.assertEqual(inventory["status"], "MATCH")
        self.assertEqual(column_diff, [])
        self.assertEqual(type_mismatch, [])


if __name__ == "__main__":
    unittest.main()
