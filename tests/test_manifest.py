import json
import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.config import AppConfig, OracleConfig, PostgresConfig
from oracle_pg_sync.manifest import RunManifest, sanitize


class ManifestTest(unittest.TestCase):
    def test_sanitize_masks_secret_values(self):
        data = {"password": "super-secret", "nested": {"api_token": "abc", "host": "db"}}

        self.assertEqual(sanitize(data)["password"], "****")
        self.assertEqual(sanitize(data)["nested"]["api_token"], "****")
        self.assertEqual(sanitize(data)["nested"]["host"], "db")

    def test_manifest_exists_and_does_not_contain_password(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.yaml"
            config_path.write_text("oracle:\n  password: super-secret\n", encoding="utf-8")
            manifest = RunManifest(
                report_dir=Path(tmp),
                run_id="run1",
                command="sync",
                config_file=str(config_path),
                config=AppConfig(
                    oracle=OracleConfig(host="oracle.local", password="super-secret", schema="APP"),
                    postgres=PostgresConfig(host="pg.local", password="pg-secret", database="db"),
                ),
                direction="oracle-to-postgres",
                dry_run=True,
                tables_requested=["public.sample"],
            )

            path = manifest.finish(
                result_rows=[{"status": "DRY_RUN", "rows_loaded": 0}],
                dependency_rows=[{"broken_count": 1, "invalid_count": 1}],
            )
            text = path.read_text(encoding="utf-8")
            data = json.loads(text)

        self.assertTrue(path.name.endswith("manifest.json"))
        self.assertNotIn("super-secret", text)
        self.assertNotIn("pg-secret", text)
        self.assertEqual(data["run_id"], "run1")
        self.assertEqual(data["dependency_summary"]["broken"], 1)


if __name__ == "__main__":
    unittest.main()
