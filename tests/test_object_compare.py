import unittest

from oracle_pg_sync.metadata.object_compare import compare_object_inventory, normalize_object_types


class ObjectCompareTest(unittest.TestCase):
    def test_compare_object_inventory_marks_missing_sides(self):
        oracle_rows = [
            {"object_type": "VIEW", "object_name": "sample_view", "parent_name": "", "status": "VALID"},
            {"object_type": "SEQUENCE", "object_name": "sample_seq", "parent_name": "", "status": ""},
        ]
        postgres_rows = [
            {"object_type": "VIEW", "object_name": "sample_view", "parent_name": "", "status": ""},
            {"object_type": "FUNCTION", "object_name": "sample_fn", "parent_name": "", "status": ""},
        ]

        rows = compare_object_inventory(oracle_rows, postgres_rows)
        statuses = {(row["object_type"], row["object_name"]): row["status"] for row in rows}

        self.assertEqual(statuses[("VIEW", "sample_view")], "MATCH")
        self.assertEqual(statuses[("SEQUENCE", "sample_seq")], "MISSING_IN_POSTGRES")
        self.assertEqual(statuses[("FUNCTION", "sample_fn")], "MISSING_IN_ORACLE")

    def test_normalize_object_types_accepts_aliases(self):
        self.assertEqual(normalize_object_types(["view", "mview", "sp"]), {"VIEW", "MATERIALIZED VIEW", "PROCEDURE"})


if __name__ == "__main__":
    unittest.main()
