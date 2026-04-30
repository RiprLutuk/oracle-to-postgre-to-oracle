import tempfile
import unittest
from pathlib import Path


class ReportsTest(unittest.TestCase):
    def test_audit_reports_do_not_write_duplicate_inventory_xlsx(self):
        from oracle_pg_sync.reports import write_audit_reports

        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp)

            write_audit_reports(
                report_dir,
                inventory_rows=[{"table_name": "public.sample", "status": "MATCH"}],
                column_diff_rows=[],
                type_mismatch_rows=[],
                dependency_rows=[],
            )

            self.assertTrue((report_dir / "inventory_summary.csv").exists())
            self.assertFalse((report_dir / "inventory_summary.xlsx").exists())
            self.assertTrue((report_dir / "report.html").exists())


if __name__ == "__main__":
    unittest.main()
