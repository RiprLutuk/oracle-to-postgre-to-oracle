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

    def test_number_and_numeric_are_compatible(self):
        oracle = ColumnMeta(name="amount", ordinal=1, data_type="NUMBER", numeric_precision=12, numeric_scale=2)
        postgres = ColumnMeta(
            name="amount",
            ordinal=1,
            data_type="numeric",
            numeric_precision=12,
            numeric_scale=2,
            udt_name="numeric",
        )

        self.assertTrue(is_type_compatible(oracle, postgres)[0])

    def test_varchar2_and_varchar_are_compatible(self):
        oracle = ColumnMeta(name="code", ordinal=1, data_type="VARCHAR2", char_length=30)
        postgres = ColumnMeta(name="code", ordinal=1, data_type="character varying", char_length=30, udt_name="varchar")

        self.assertTrue(is_type_compatible(oracle, postgres)[0])

    def test_common_oracle_postgres_aliases_are_compatible(self):
        cases = [
            (
                ColumnMeta(name="id", ordinal=1, data_type="NUMBER", numeric_precision=4, numeric_scale=0),
                ColumnMeta(name="id", ordinal=1, data_type="smallint", udt_name="int2"),
            ),
            (
                ColumnMeta(name="flag", ordinal=1, data_type="BOOLEAN"),
                ColumnMeta(name="flag", ordinal=1, data_type="boolean", udt_name="bool"),
            ),
            (
                ColumnMeta(name="payload", ordinal=1, data_type="RAW"),
                ColumnMeta(name="payload", ordinal=1, data_type="bytea", udt_name="bytea"),
            ),
            (
                ColumnMeta(name="duration", ordinal=1, data_type="INTERVAL DAY TO SECOND"),
                ColumnMeta(name="duration", ordinal=1, data_type="interval", udt_name="interval"),
            ),
            (
                ColumnMeta(name="rid", ordinal=1, data_type="ROWID"),
                ColumnMeta(name="rid", ordinal=1, data_type="text", udt_name="text"),
            ),
            (
                ColumnMeta(name="doc", ordinal=1, data_type="JSON"),
                ColumnMeta(name="doc", ordinal=1, data_type="jsonb", udt_name="jsonb"),
            ),
            (
                ColumnMeta(name="doc", ordinal=1, data_type="XMLTYPE"),
                ColumnMeta(name="doc", ordinal=1, data_type="text", udt_name="text"),
            ),
        ]

        for oracle, postgres in cases:
            with self.subTest(oracle=oracle.data_type, postgres=postgres.data_type):
                self.assertTrue(is_type_compatible(oracle, postgres)[0])


if __name__ == "__main__":
    unittest.main()
