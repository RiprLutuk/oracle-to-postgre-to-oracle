import unittest
import sys
import types

if "psycopg" not in sys.modules:
    psycopg_stub = types.ModuleType("psycopg")
    psycopg_stub.connect = lambda *args, **kwargs: None
    psycopg_stub.sql = types.SimpleNamespace(
        SQL=lambda value: value,
        Identifier=lambda value: value,
        Literal=lambda value: value,
    )
    sys.modules["psycopg"] = psycopg_stub

if "oracledb" not in sys.modules:
    oracledb_stub = types.ModuleType("oracledb")
    oracledb_stub.connect = lambda *args, **kwargs: None
    oracledb_stub.init_oracle_client = lambda *args, **kwargs: None
    oracledb_stub.makedsn = lambda host, port, service_name=None, sid=None: "oracle-dsn"
    sys.modules["oracledb"] = oracledb_stub

from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig, SyncConfig
from oracle_pg_sync.sync.oracle_to_postgres import OracleToPostgresSync


class OracleToPostgresSyncTest(unittest.TestCase):
    def test_swap_execute_is_guarded_by_default(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(allow_swap=False),
            )
        )

        message = sync._swap_guard_message("public.sample_customer", 1024, force=False)

        self.assertIn("mode swap dinonaktifkan", message)

    def test_swap_max_size_accepts_force_bypass(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(allow_swap=True, max_swap_table_bytes=1024),
            )
        )

        message = sync._swap_guard_message("public.sample_customer", 2048, force=True)

        self.assertEqual(message, "")

    def test_swap_dry_run_mentions_estimated_storage(self):
        sync = OracleToPostgresSync(
            AppConfig(
                oracle=OracleConfig(schema="APP"),
                postgres=PostgresConfig(schema="public"),
                sync=SyncConfig(swap_space_multiplier=2.5),
            )
        )

        message = sync._dry_run_message("public.sample_customer", "swap", 3, 1024**3)

        self.assertIn("2.5 GiB", message)


if __name__ == "__main__":
    unittest.main()
