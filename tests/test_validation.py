import unittest
from datetime import datetime
from decimal import Decimal

from oracle_pg_sync.metadata.type_mapping import ColumnMeta
from oracle_pg_sync.validation import checksum_columns, stable_row_hash


class ValidationTest(unittest.TestCase):
    def test_matching_checksum(self):
        rows = [(1, "Alice", None), (2, "Bob", Decimal("10.0"))]

        self.assertEqual(stable_row_hash(rows, ["id", "name", "amount"]), stable_row_hash(rows, ["id", "name", "amount"]))

    def test_mismatched_checksum(self):
        left = [(1, "Alice")]
        right = [(1, "Alicia")]

        self.assertNotEqual(stable_row_hash(left, ["id", "name"]), stable_row_hash(right, ["id", "name"]))

    def test_excludes_lob_column(self):
        columns = [
            ColumnMeta("ID", 1, "NUMBER"),
            ColumnMeta("BLOB_PAYLOAD", 2, "BLOB"),
            ColumnMeta("NOTE", 3, "CLOB"),
        ]

        self.assertEqual(checksum_columns(columns), ["id"])

    def test_null_date_decimal_normalization_is_stable(self):
        rows1 = [(None, datetime(2026, 1, 1, 12, 0, 0), Decimal("1.0"))]
        rows2 = [(None, datetime(2026, 1, 1, 12, 0, 0), Decimal("1.00"))]

        self.assertEqual(stable_row_hash(rows1, ["empty", "ts", "num"]), stable_row_hash(rows2, ["empty", "ts", "num"]))


if __name__ == "__main__":
    unittest.main()
