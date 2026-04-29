import tempfile
import unittest
import os
from pathlib import Path
from unittest.mock import patch

from oracle_pg_sync.config import PostgresConfig, load_config, validate_postgres_config


class ConfigTest(unittest.TestCase):
    def test_dotenv_is_loaded_automatically(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".env").write_text("PG_HOST=pg-from-dotenv\nPG_USER=dotenv-user\n", encoding="utf-8")
            path = root / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  host: ${PG_HOST}
  user: ${PG_USER}
  schema: public
""",
                encoding="utf-8",
            )
            cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch.dict(os.environ, {}, clear=True):
                    config = load_config(path)
            finally:
                os.chdir(cwd)

        self.assertEqual(config.postgres.host, "pg-from-dotenv")
        self.assertEqual(config.postgres.user, "dotenv-user")

    def test_missing_env_var_fails_fast(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  host: ${PG_HOST}
""",
                encoding="utf-8",
            )

            cwd = os.getcwd()
            try:
                os.chdir(root)
                with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(RuntimeError, "PG_HOST is not set"):
                    load_config(path)
            finally:
                os.chdir(cwd)

    def test_optional_oracle_dsn_can_be_unset_when_host_is_used(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yaml"
            path.write_text(
                """
oracle:
  dsn: ${ORACLE_DSN}
  host: ${ORACLE_HOST}
  port: ${ORACLE_PORT}
  user: ${ORACLE_USER}
  password: ${ORACLE_PASSWORD}
postgres:
  host: pg.local
""",
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ORACLE_HOST": "oracle.local",
                    "ORACLE_USER": "app",
                    "ORACLE_PASSWORD": "pw",
                },
                clear=True,
            ):
                config = load_config(path)

        self.assertEqual(config.oracle.dsn, "")
        self.assertEqual(config.oracle.host, "oracle.local")
        self.assertEqual(config.oracle.port, "1521")

    def test_custom_env_file_is_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_file = root / ".env.prod"
            env_file.write_text("PG_HOST=pg-prod\n", encoding="utf-8")
            path = root / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  host: ${PG_HOST}
""",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True):
                config = load_config(path, env_file=env_file)

        self.assertEqual(config.postgres.host, "pg-prod")

    def test_exported_env_is_not_overridden_by_dotenv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_file = root / ".env"
            env_file.write_text("PG_HOST=pg-from-dotenv\n", encoding="utf-8")
            path = root / "config.yaml"
            path.write_text(
                """
oracle:
  schema: APP
postgres:
  host: ${PG_HOST}
""",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"PG_HOST": "pg-exported"}, clear=True):
                config = load_config(path, env_file=env_file)

        self.assertEqual(config.postgres.host, "pg-exported")

    def test_missing_postgres_host_validation_prevents_connection_fallback(self):
        with self.assertRaisesRegex(RuntimeError, "PG_HOST is not set"):
            validate_postgres_config(PostgresConfig(host=None, database="app", user="user", password="pw"))

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
        self.assertTrue(config.validation.rowcount.enabled)

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

    def test_table_resolution_matches_source_and_target_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.yaml"
            path.write_text(
                """
oracle:
  schema: PRD_AMSPBRIM
postgres:
  schema: public
tables:
  - name: batch_config
    source_schema: PRD_AMSPBRIM
    source_table: A_HP_BATCH
    target_schema: public
    target_table: a_hp_batch
""",
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertEqual(config.resolve_table_name("A_HP_BATCH"), "batch_config")
        self.assertEqual(config.resolve_table_name("public.a_hp_batch"), "batch_config")

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
  fail_on_broken_dependency: false
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
        self.assertFalse(config.dependency.fail_on_broken_dependency)
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
