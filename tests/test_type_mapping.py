import unittest

from oracle_pg_sync.metadata.type_mapping import ColumnMeta, is_type_compatible, suggested_pg_type


class TypeMappingTest(unittest.TestCase):
    def test_varchar_pg_text_is_compatible(self):
        oracle = ColumnMeta(name="name", ordinal=1, data_type="VARCHAR2", char_length=20)
        postgres = ColumnMeta(name="name", ordinal=1, data_type="text", udt_name="text")

        self.assertTrue(is_type_compatible(oracle, postgres)[0])

    def test_number_integer_precision_guard(self):
        oracle = ColumnMeta(name="id", ordinal=1, data_type="NUMBER", numeric_precision=18, numeric_scale=0)
        postgres = ColumnMeta(name="id", ordinal=1, data_type="integer", udt_name="int4")

        ok, reason = is_type_compatible(oracle, postgres)

        self.assertFalse(ok)
        self.assertIn("too large", reason)

    def test_suggest_oracle_clob_to_text(self):
        oracle = ColumnMeta(name="notes", ordinal=1, data_type="CLOB")

        self.assertEqual(suggested_pg_type(oracle), "text")


if __name__ == "__main__":
    unittest.main()
