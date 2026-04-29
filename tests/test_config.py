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

    def test_load_rich_lob_strategy_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  schema: public
lob_strategy:
  default: error
  stream_batch_size: 100
  lob_chunk_size_bytes: 1048576
  validation:
    default: size
    hash_algorithm: sha256
tables:
  - name: public.sample_blob_table
    lob_strategy:
      columns:
        HOUSE_IMAGE:
          strategy: stream
          target_type: bytea
          validation: size_hash
""",
                encoding="utf-8",
            )

            config = load_config(path)

        table = config.table_config("sample_blob_table")
        self.assertEqual(config.lob_strategy.stream_batch_size, 100)
        self.assertEqual(config.lob_strategy.lob_chunk_size_bytes, 1048576)
        self.assertEqual(config.lob_strategy.validation["hash_algorithm"], "sha256")
        self.assertEqual(table.lob_strategy.columns["HOUSE_IMAGE"].strategy, "stream")
        self.assertEqual(table.lob_strategy.columns["HOUSE_IMAGE"].target_type, "bytea")
        self.assertEqual(table.lob_strategy.columns["HOUSE_IMAGE"].validation, "size_hash")

    def test_load_tables_from_tables_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            (root / "configs" / "tables.yaml").write_text(
                """
tables:
  - name: public.from_file
    directions:
      - oracle-to-postgres
""",
                encoding="utf-8",
            )
            path = root / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  schema: public
tables_file: configs/tables.yaml
""",
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertEqual(config.tables_file, Path("configs/tables.yaml"))
        self.assertEqual(config.table_names(), ["public.from_file"])

    def test_load_dependency_and_job_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  schema: public
dependency:
  auto_recompile_oracle: false
  refresh_postgres_mview: false
  max_recompile_attempts: 5
job:
  retry: 4
  timeout_seconds: 7200
  alert_command: echo ALERT
""",
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertFalse(config.dependency.auto_recompile_oracle)
        self.assertFalse(config.dependency.refresh_postgres_mview)
        self.assertEqual(config.dependency.max_recompile_attempts, 5)
        self.assertEqual(config.job.retry, 4)
        self.assertEqual(config.job.timeout_seconds, 7200)
        self.assertEqual(config.job.alert_command, "echo ALERT")

    def test_tables_file_and_inline_tables_are_mutually_exclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            (root / "configs" / "tables.yaml").write_text("tables: []\n", encoding="utf-8")
            path = root / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  schema: public
tables_file: configs/tables.yaml
tables:
  - name: public.inline
""",
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                load_config(path)


if __name__ == "__main__":
    unittest.main()
