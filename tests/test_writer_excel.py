import tempfile
import unittest
from pathlib import Path

pytest_import_error = None
try:
    from openpyxl import load_workbook
except ModuleNotFoundError as exc:
    pytest_import_error = exc


@unittest.skipIf(pytest_import_error is not None, "openpyxl is not installed")
class WriterExcelTest(unittest.TestCase):
    def test_central_report_has_required_sheets(self):
        from oracle_pg_sync.reports.writer_excel import write_central_report_xlsx

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.xlsx"

            write_central_report_xlsx(
                path,
                sync_rows=[{"table_name": "public.sample", "status": "SUCCESS", "rows_loaded": 10}],
                checksum_rows=[{"table_name": "public.sample", "chunk_key": "table", "status": "MATCH"}],
                config_sanitized={"oracle": {"password": "****"}},
            )

            workbook = load_workbook(path)

        self.assertIn("00_Dashboard", workbook.sheetnames)
        self.assertIn("04_Checksum_Result", workbook.sheetnames)
        self.assertIn("14_Config_Sanitized", workbook.sheetnames)
        self.assertEqual(len(workbook.sheetnames), 15)


if __name__ == "__main__":
    unittest.main()
