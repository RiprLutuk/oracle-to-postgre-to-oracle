import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.config import load_config


class ConfigTest(unittest.TestCase):
    def test_load_config_defaults_and_table_lookup(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yaml"
            path.write_text(
                """
oracle:
  host: oracle.local
  port: 1521
  service_name: ORCLPDB1
  user: app
  password: REPLACE_ME
  schema: APP
postgres:
  host: pg.local
  database: appdb
  user: app
  password: REPLACE_ME
sync:
  allow_swap: false
  max_swap_table_bytes: 10GiB
tables:
  - name: public.sample_customer
    mode: truncate
    incremental:
      enabled: true
      strategy: updated_at
      column: updated_at
    lob_strategy:
      columns:
        BLOB_PAYLOAD: null
    validation:
      checksum:
        enabled: true
        exclude_columns:
          - BLOB_PAYLOAD
""",
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertEqual(config.postgres.schema, "public")
        self.assertTrue(config.sync.dry_run)
        self.assertFalse(config.sync.allow_swap)
        self.assertEqual(config.sync.max_swap_table_bytes, 10 * 1024**3)
        self.assertEqual(config.table_config("sample_customer").mode, "truncate")
        table = config.table_config("sample_customer")
        self.assertTrue(table.incremental.enabled)
        self.assertEqual(table.incremental.column, "updated_at")
        self.assertEqual(table.lob_strategy.columns["BLOB_PAYLOAD"], "null")
        self.assertTrue(table.validation.checksum.enabled)


if __name__ == "__main__":
    unittest.main()
