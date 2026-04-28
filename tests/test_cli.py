import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.cli import _resolve_tables, build_parser
from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig, TableConfig


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


if __name__ == "__main__":
    unittest.main()
