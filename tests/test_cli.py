import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.cli import _resolve_tables
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


if __name__ == "__main__":
    unittest.main()
