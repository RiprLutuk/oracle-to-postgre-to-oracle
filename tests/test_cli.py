import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from oracle_pg_sync.cli import (
    _apply_lob_override,
    _apply_profile,
    _apply_runtime_table_overrides,
    _apply_where_override,
    _latest_run_dir,
    _resolve_tables,
    _run_report_files,
    build_parser,
    main as cli_main,
)
from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig, TableConfig
from oracle_pg_sync.ops import _expand_bare_lob_flag, main as ops_main


class CliTest(unittest.TestCase):
    def test_resolve_tables_from_file_filters_direction_and_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            tables_file = Path(tmp) / "tables.yaml"
            tables_file.write_text(
                """
tables:
  - name: public.sample_customer
    directions:
      - oracle-to-postgres
  - name: public.sample_order
    directions:
      - postgres-to-oracle
  - name: public.sample_audit_log
    directions:
      - oracle-to-postgres
""",
                encoding="utf-8",
            )

            tables = _resolve_tables(
                AppConfig(oracle=OracleConfig(), postgres=PostgresConfig()),
                None,
                direction="oracle-to-postgres",
                tables_file=str(tables_file),
                limit=1,
            )

        self.assertEqual(tables, ["public.sample_customer"])

    def test_manual_tables_override_config_tables(self):
        config = AppConfig(
            oracle=OracleConfig(),
            postgres=PostgresConfig(),
            tables=[TableConfig(name="public.from_config")],
        )

        tables = _resolve_tables(config, ["sample_customer", "sample_order"], direction="oracle-to-postgres")

        self.assertEqual(tables, ["sample_customer", "sample_order"])

    def test_audit_accepts_all_postgres_tables_flag(self):
        args = build_parser().parse_args(["audit", "--all-postgres-tables", "--limit", "10"])

        self.assertTrue(args.all_postgres_tables)
        self.assertEqual(args.limit, 10)

    def test_audit_objects_command_accepts_types(self):
        args = build_parser().parse_args(
            ["audit-objects", "--types", "view", "sequence", "--include-extension-objects"]
        )

        self.assertEqual(args.command, "audit-objects")
        self.assertEqual(args.types, ["view", "sequence"])
        self.assertTrue(args.include_extension_objects)

    def test_dependencies_command_accepts_manual_tables(self):
        args = build_parser().parse_args(["dependencies", "--tables", "SAMPLE_CUSTOMER", "SAMPLE_LOCATION"])

        self.assertEqual(args.command, "dependencies")
        self.assertEqual(args.tables, ["SAMPLE_CUSTOMER", "SAMPLE_LOCATION"])

    def test_sync_accepts_checkpoint_incremental_and_watermark_flags(self):
        args = build_parser().parse_args(
            [
                "sync",
                "--resume",
                "run123",
                "--incremental",
                "--watermark-status",
                "--reset-watermark",
                "public.sample",
            ]
        )

        self.assertEqual(args.resume, "run123")
        self.assertTrue(args.incremental)
        self.assertTrue(args.watermark_status)
        self.assertEqual(args.reset_watermark, "public.sample")

    def test_sync_accepts_where_override(self):
        args = build_parser().parse_args(
            [
                "sync",
                "--direction",
                "postgres-to-oracle",
                "--mode",
                "upsert",
                "--where",
                "updated_at >= NOW() - INTERVAL '5 minutes'",
                "--tables",
                "sample_customer",
            ]
        )

        self.assertEqual(args.where, "updated_at >= NOW() - INTERVAL '5 minutes'")

    def test_sync_accepts_key_and_incremental_overrides(self):
        args = build_parser().parse_args(
            [
                "sync",
                "--direction",
                "postgres-to-oracle",
                "--tables",
                "public.address",
                "--mode",
                "upsert",
                "--key-columns",
                "address_id",
                "--incremental-column",
                "last_update",
                "--initial-value",
                "2026-01-01T00:00:00",
                "--overlap-minutes",
                "10",
            ]
        )

        self.assertEqual(args.key_columns, ["address_id"])
        self.assertEqual(args.incremental_column, "last_update")
        self.assertEqual(args.initial_value, "2026-01-01T00:00:00")
        self.assertEqual(args.overlap_minutes, 10)

    def test_sync_accepts_go_and_lob_override(self):
        args = build_parser().parse_args(["sync", "--go", "--lob", "stream"])

        self.assertTrue(args.execute)
        self.assertEqual(args.lob, "stream")

    def test_sync_accepts_safe_modes_and_simulate(self):
        args = build_parser().parse_args(["sync", "--mode", "truncate_safe", "--simulate"])

        self.assertEqual(args.mode, "truncate_safe")
        self.assertTrue(args.simulate)

    def test_sync_accepts_profiles_and_lock_flags(self):
        args = build_parser().parse_args(["sync", "--profile", "every_5min", "--lock-file", "reports/job.lock"])

        self.assertEqual(args.profile, "every_5min")
        self.assertEqual(args.lock_file, "reports/job.lock")

    def test_profile_every_5min_sets_incremental_upsert(self):
        args = build_parser().parse_args(["sync", "--profile", "every_5min"])

        _apply_profile(args)

        self.assertTrue(args.incremental)
        self.assertEqual(args.mode, "incremental_safe")

    def test_where_override_updates_table_config(self):
        config = AppConfig(
            oracle=OracleConfig(),
            postgres=PostgresConfig(),
            tables=[TableConfig(name="public.sample_customer")],
        )

        _apply_where_override(config, ["sample_customer"], "updated_at >= '2026-01-01'")

        self.assertEqual(config.tables[0].where, "updated_at >= '2026-01-01'")

    def test_runtime_overrides_create_reverse_table_details_from_simple_table_list(self):
        config = AppConfig(
            oracle=OracleConfig(),
            postgres=PostgresConfig(),
            tables=[TableConfig(name="public.address")],
        )
        args = build_parser().parse_args(
            [
                "sync",
                "--direction",
                "postgres-to-oracle",
                "--tables",
                "public.address",
                "--mode",
                "upsert",
                "--key-columns",
                "address_id",
                "--incremental-column",
                "last_update",
                "--where",
                "last_update >= CURRENT_DATE",
            ]
        )

        _apply_runtime_table_overrides(args, config, ["public.address"])

        table_cfg = config.table_config("public.address")
        self.assertEqual(table_cfg.key_columns, ["address_id"])
        self.assertEqual(table_cfg.where, "last_update >= CURRENT_DATE")
        self.assertTrue(table_cfg.incremental.enabled)
        self.assertEqual(table_cfg.incremental.column, "last_update")

    def test_lob_override_updates_default_strategy(self):
        config = AppConfig(oracle=OracleConfig(), postgres=PostgresConfig())

        _apply_lob_override(config, "include")

        self.assertEqual(config.lob_strategy.default, "stream")

    def test_ops_validate_bare_lob_defaults_to_stream(self):
        self.assertEqual(_expand_bare_lob_flag(["--lob", "--tables", "sample"]), ["--lob", "stream", "--tables", "sample"])

    def test_ops_report_latest_without_reports_is_successful(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text(
                f"""
oracle:
  schema: APP
postgres:
  schema: public
reports:
  output_dir: {tmp}/reports
""",
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()):
                self.assertEqual(ops_main(["report", "latest", "--config", str(config_path)]), 0)

    def test_latest_run_dir_and_report_files_are_run_scoped(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports"
            old_run = report_dir / "run_20260101_010101_old"
            new_run = report_dir / "run_20260102_010101_new"
            old_run.mkdir(parents=True)
            new_run.mkdir(parents=True)
            (old_run / "manifest.json").write_text("{}", encoding="utf-8")
            (new_run / "manifest.json").write_text("{}", encoding="utf-8")
            (new_run / "report.html").write_text("", encoding="utf-8")
            (new_run / "report.xlsx").write_text("", encoding="utf-8")

            latest = _latest_run_dir(report_dir)
            files = _run_report_files(new_run, "report.html", "report.xlsx", "missing.csv")

        self.assertEqual(latest, new_run)
        self.assertEqual(files, [str(new_run / "report.html"), str(new_run / "report.xlsx")])

    def test_report_command_regenerates_latest_run_html(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp) / "reports"
            run_dir = report_dir / "run_20260102_010101_new"
            run_dir.mkdir(parents=True)
            (run_dir / "manifest.json").write_text("{}", encoding="utf-8")
            (run_dir / "report.xlsx").write_text("", encoding="utf-8")
            (run_dir / "inventory_summary.csv").write_text(
                "table_name,oracle_row_count,postgres_row_count,row_count_match,status\npublic.sample,1,1,true,MATCH\n",
                encoding="utf-8",
            )
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text(
                f"""
oracle:
  schema: APP
postgres:
  schema: public
reports:
  output_dir: {report_dir}
""",
                encoding="utf-8",
            )

            self.assertEqual(cli_main(["report", "--config", str(config_path)]), 0)
            html = (run_dir / "report.html").read_text(encoding="utf-8")

        self.assertIn("public.sample", html)
        self.assertIn('href="manifest.json"', html)
        self.assertIn('href="report.xlsx"', html)


if __name__ == "__main__":
    unittest.main()
