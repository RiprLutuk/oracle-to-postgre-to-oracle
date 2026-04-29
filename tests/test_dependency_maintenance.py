import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.cli import _dependency_failed, _run_dependency_maintenance, _write_dependency_summary
from oracle_pg_sync.config import AppConfig, DependencyConfig, OracleConfig, PostgresConfig
from oracle_pg_sync.dependency_health import critical_dependency_rows, summarize_dependency_rows


class OracleDependencyMaintenanceTest(unittest.TestCase):
    def test_compile_invalid_objects_builds_safe_compile_statements(self):
        class Cursor:
            def __init__(self):
                self.statements = []

            def execute(self, statement, params=None):
                self.statements.append(str(statement))

            def fetchall(self):
                return [
                    ("VIEW", "V_SAMPLE", "INVALID"),
                    ("PACKAGE BODY", "PKG_SAMPLE", "INVALID"),
                ]

        cur = Cursor()

        rows = oracle.compile_invalid_objects(cur, "APP")

        self.assertEqual(rows[0]["compile_status"], "attempted")
        self.assertIn('ALTER VIEW "APP"."V_SAMPLE" COMPILE', cur.statements)
        self.assertIn('ALTER PACKAGE "APP"."PKG_SAMPLE" COMPILE BODY', cur.statements)


class PostgresDependencyMaintenanceTest(unittest.TestCase):
    def test_refresh_materialized_views_deduplicates_dependencies(self):
        class Cursor:
            def __init__(self):
                self.executed = []

            def execute(self, statement, params=None):
                self.executed.append(statement)

        cur = Cursor()

        rows = postgres.refresh_materialized_views(
            cur,
            [
                {"object_schema": "public", "object_name": "mv_sales", "object_type": "MATERIALIZED VIEW"},
                {"object_schema": "public", "object_name": "mv_sales", "object_type": "MATERIALIZED VIEW"},
                {"object_schema": "public", "object_name": "v_sales", "object_type": "VIEW"},
            ],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(cur.executed), 1)
        self.assertEqual(rows[0]["maintenance_status"], "refreshed")

    def test_function_dependency_rows_fall_back_to_function_definition_heuristic(self):
        class Cursor:
            def execute(self, statement, params=None):
                sql_text = str(statement)
                if "JOIN pg_proc p ON p.oid = d.objid" in sql_text:
                    self._rows = []
                elif "pg_get_functiondef" in sql_text:
                    self._rows = [
                        ("public", "fn_sample", "f", "", "CREATE FUNCTION public.fn_sample() RETURNS int LANGUAGE sql AS $$ SELECT COUNT(*) FROM public.sample $$;"),
                    ]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        cur = Cursor()

        rows = postgres._function_dependency_rows(cur, "public", "sample")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["object_type"], "FUNCTION")
        self.assertEqual(rows[0]["dependency_kind"], "function_definition_reference")


class DependencyLifecycleTest(unittest.TestCase):
    def test_maintenance_order_is_refresh_compile_validate(self):
        calls = []

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return self

            def commit(self):
                calls.append("commit")

        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch("oracle_pg_sync.db.oracle.connect", return_value=Conn()),
                patch("oracle_pg_sync.db.postgres.connect", return_value=Conn()),
                patch(
                    "oracle_pg_sync.db.postgres.refresh_materialized_views",
                    side_effect=lambda cur, rows: calls.append("refresh") or [],
                ),
                patch(
                    "oracle_pg_sync.db.oracle.compile_invalid_objects",
                    side_effect=lambda cur, owner: calls.append("compile") or [],
                ),
                patch(
                    "oracle_pg_sync.db.postgres.validate_dependent_objects",
                    side_effect=lambda cur, rows: calls.append("validate") or [],
                ),
            ):
                _run_dependency_maintenance(
                    AppConfig(oracle=OracleConfig(schema="APP"), postgres=PostgresConfig(schema="public")),
                    ["public.sample"],
                    __import__("logging").getLogger("test_dependency_lifecycle"),
                    Path(tmp),
                    [],
                    execute=True,
                )

        self.assertEqual(calls, ["refresh", "compile", "commit", "validate"])

    def test_maintenance_respects_dependency_config(self):
        calls = []

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return self

            def commit(self):
                calls.append("commit")

        config = AppConfig(
            oracle=OracleConfig(schema="APP"),
            postgres=PostgresConfig(schema="public"),
            dependency=DependencyConfig(
                auto_recompile_oracle=False,
                refresh_postgres_mview=False,
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch("oracle_pg_sync.db.oracle.connect", return_value=Conn()),
                patch("oracle_pg_sync.db.postgres.connect", return_value=Conn()),
                patch(
                    "oracle_pg_sync.db.postgres.refresh_materialized_views",
                    side_effect=lambda *args: calls.append("refresh") or [],
                ),
                patch(
                    "oracle_pg_sync.db.oracle.compile_invalid_objects",
                    side_effect=lambda *args: calls.append("compile") or [],
                ),
                patch(
                    "oracle_pg_sync.db.postgres.validate_dependent_objects",
                    side_effect=lambda *args: calls.append("validate") or [],
                ),
            ):
                _run_dependency_maintenance(
                    config,
                    ["public.sample"],
                    __import__("logging").getLogger("test_dependency_config"),
                    Path(tmp),
                    [],
                    execute=True,
                )

        self.assertEqual(calls, ["commit", "validate"])

    def test_dependency_summary_marks_broken_rows(self):
        rows = summarize_dependency_rows(
            [
                {
                    "phase": "pre",
                    "source_db": "oracle",
                    "table_name": "public.sample",
                    "status": "INVALID",
                }
            ],
            [
                {
                    "source_db": "postgres",
                    "table_name": "public.sample",
                    "validation_status": "missing",
                }
            ],
        )

        self.assertEqual(sum(row["broken_count"] for row in rows), 2)
        self.assertEqual(len(critical_dependency_rows(rows)), 2)

    def test_dependency_fail_policy_can_be_disabled(self):
        row = {"status": "INVALID"}
        strict = AppConfig(oracle=OracleConfig(), postgres=PostgresConfig())
        relaxed = AppConfig(
            oracle=OracleConfig(),
            postgres=PostgresConfig(),
            dependency=DependencyConfig(fail_on_broken_dependency=False),
        )

        self.assertTrue(_dependency_failed(strict, [row]))
        self.assertFalse(_dependency_failed(relaxed, [row]))

    def test_write_dependency_summary_creates_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = _write_dependency_summary(
                Path(tmp),
                [{"phase": "pre", "source_db": "oracle", "table_name": "public.sample"}],
                [],
            )

            content = (Path(tmp) / "dependency_summary.csv").read_text(encoding="utf-8")

        self.assertEqual(rows[0]["object_count"], 1)
        self.assertIn("object_count", content)


if __name__ == "__main__":
    unittest.main()
