import unittest

from oracle_pg_sync.db import oracle


class FakeCursor:
    def __init__(self, rows_by_name):
        self.rows_by_name = rows_by_name
        self.result = None
        self.executed = []

    def execute(self, query, params=None):
        params = params or {}
        self.executed.append((query, params))
        lookup = None
        if "LOWER(TABLE_NAME)" in query or "LOWER(OBJECT_NAME)" in query:
            lookup = str(params["name"]).lower()
        elif "TABLE_NAME = :name" in query or "OBJECT_NAME = :name" in query:
            lookup = str(params["name"])
        self.result = self.rows_by_name.get(lookup)

    def fetchone(self):
        if self.result is None:
            return None
        return (self.result,)


class OracleNameResolutionTest(unittest.TestCase):
    def test_resolve_table_name_falls_back_to_case_insensitive_lookup(self):
        cur = FakeCursor({"samplemixedcase": "SampleMixedCase"})

        self.assertEqual(oracle.resolve_table_name(cur, "SAMPLE_APP", "samplemixedcase"), "SampleMixedCase")

    def test_table_exists_supports_quoted_mixed_case_oracle_tables(self):
        cur = FakeCursor({"samplemixedcase": "SampleMixedCase"})

        self.assertTrue(oracle.table_exists(cur, "SAMPLE_APP", "samplemixedcase"))

    def test_resolve_table_name_prefers_uppercase_exact_match(self):
        cur = FakeCursor({"SAMPLE_TABLE": "SAMPLE_TABLE", "sample_table": "Sample_Table"})

        self.assertEqual(oracle.resolve_table_name(cur, "SAMPLE_APP", "sample_table"), "SAMPLE_TABLE")


if __name__ == "__main__":
    unittest.main()
