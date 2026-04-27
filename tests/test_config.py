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
tables:
  - name: public.ADDRESS
    mode: swap
""",
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertEqual(config.postgres.schema, "public")
        self.assertTrue(config.sync.dry_run)
        self.assertEqual(config.table_config("ADDRESS").mode, "swap")


if __name__ == "__main__":
    unittest.main()
