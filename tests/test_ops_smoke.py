import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from oracle_pg_sync.ops import main as ops_main


class OpsSmokeTest(unittest.TestCase):
    def test_ops_help(self):
        with redirect_stdout(StringIO()) as output:
            status = ops_main(["--help"])

        self.assertEqual(status, 0)
        self.assertIn("ops sync --go", output.getvalue())

    def test_ops_sync_dry_run_smoke_writes_run_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            reports_dir = Path(tmp) / "reports"
            config_path.write_text(
                f"""
oracle:
  schema: APP
postgres:
  schema: public
reports:
  output_dir: {reports_dir}
sync:
  checkpoint_dir: {reports_dir}/checkpoints/checkpoint.sqlite3
tables:
  - public.sample
""",
                encoding="utf-8",
            )

            with patch("oracle_pg_sync.cli._write_dependency_report", return_value=[]), patch(
                "oracle_pg_sync.cli._run_dependency_maintenance",
                return_value=[],
            ), patch("oracle_pg_sync.cli._sync_runner", return_value=_FakeRunner()):
                status = ops_main(
                    [
                        "sync",
                        "--config",
                        str(config_path),
                        "--tables",
                        "public.sample",
                        "--lock-file",
                        str(reports_dir / "smoke.lock"),
                    ]
                )

            run_dirs = sorted(reports_dir.glob("run_*"))
            self.assertEqual(status, 0)
            self.assertEqual(len(run_dirs), 1)
            self.assertTrue((run_dirs[0] / "manifest.json").exists())
            self.assertTrue((run_dirs[0] / "sync_result.csv").exists())
            self.assertTrue((run_dirs[0] / "report.xlsx").exists())
            self.assertTrue((run_dirs[0] / "report.html").exists())

    def test_ops_report_latest_smoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_dir = Path(tmp) / "reports"
            run_dir = reports_dir / "run_20260101_000000_smoke"
            run_dir.mkdir(parents=True)
            (run_dir / "manifest.json").write_text("{}", encoding="utf-8")
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text(
                f"""
oracle:
  schema: APP
postgres:
  schema: public
reports:
  output_dir: {reports_dir}
""",
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()) as output:
                status = ops_main(["report", "latest", "--config", str(config_path)])

        self.assertEqual(status, 0)
        self.assertIn("manifest_path", output.getvalue())

    def test_ops_doctor_offline_smoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_dir = Path(tmp) / "reports"
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text(
                f"""
oracle:
  schema: APP
postgres:
  schema: public
reports:
  output_dir: {reports_dir}
sync:
  checkpoint_dir: {reports_dir}/checkpoints/checkpoint.sqlite3
tables:
  - public.sample
""",
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()) as output:
                status = ops_main(["doctor", "--offline", "--config", str(config_path)])

        self.assertEqual(status, 0)
        self.assertIn("oracle_connection,WARNING,skipped by --offline", output.getvalue())

    def test_ops_dependencies_check_delegates_to_old_cli(self):
        with patch("oracle_pg_sync.ops.cli_main", return_value=0) as cli:
            status = ops_main(["dependencies", "check", "--config", "config.yaml", "--tables", "public.sample"])

        self.assertEqual(status, 0)
        cli.assert_called_once_with(["dependencies", "--config", "config.yaml", "--tables", "public.sample"])

    def test_ops_analyze_lob_smoke_writes_run_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_dir = Path(tmp) / "reports"
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text(
                f"""
oracle:
  schema: APP
postgres:
  schema: public
reports:
  output_dir: {reports_dir}
sync:
  checkpoint_dir: {reports_dir}/checkpoints/checkpoint.sqlite3
tables:
  - public.sample
""",
                encoding="utf-8",
            )

            with patch(
                "oracle_pg_sync.lob_analysis.analyze_lob_columns",
                return_value=[
                    {
                        "source_db": "oracle",
                        "table_name": "public.sample",
                        "classification": "binary-heavy",
                        "column_name": "payload",
                        "lob_type": "BLOB",
                        "target_type": "bytea",
                        "strategy": "error",
                    }
                ],
            ):
                status = ops_main(["analyze", "lob", "--config", str(config_path)])

            run_dirs = sorted(reports_dir.glob("run_*"))
            lob_csv_exists = (run_dirs[0] / "lob_analysis.csv").exists()
            xlsx_exists = (run_dirs[0] / "report.xlsx").exists()
            html_exists = (run_dirs[0] / "report.html").exists()

        self.assertEqual(status, 0)
        self.assertTrue(lob_csv_exists)
        self.assertTrue(xlsx_exists)
        self.assertTrue(html_exists)


class _FakeRunner:
    def sync_tables(self, *args, **kwargs):
        return [_FakeResult()]


class _FakeResult:
    checksum_rows = []

    def as_row(self):
        return {
            "run_id": "smoke",
            "table_name": "public.sample",
            "direction": "oracle-to-postgres",
            "mode": "truncate",
            "status": "DRY_RUN",
            "rows_loaded": 0,
            "dry_run": True,
            "message": "smoke",
        }


if __name__ == "__main__":
    unittest.main()
