import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class JobScriptsTest(unittest.TestCase):
    def test_cron_entrypoint_scripts_are_executable(self):
        for script in ("daily.sh", "incremental.sh", "every_5min.sh"):
            mode = (ROOT / "jobs" / script).stat().st_mode
            self.assertTrue(mode & stat.S_IXUSR, script)

    def test_daily_job_supports_oracle_to_pg_direction(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            capture = tmp_path / "daily_args.txt"
            fake_python = _fake_python(tmp_path, capture)
            env = _job_env(tmp_path, fake_python)

            result = subprocess.run(
                ["bash", str(ROOT / "jobs/daily.sh"), "oracle_to_pg", "--tables", "public.sample"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            args = capture.read_text(encoding="utf-8")
            self.assertIn("-m oracle_pg_sync.ops sync", args)
            self.assertIn("--profile daily", args)
            self.assertIn("--direction oracle-to-postgres", args)
            self.assertIn("--tables public.sample", args)
            self.assertIn(f"--lock-file {tmp_path / 'locks/daily_oracle_to_pg.lock'}", args)

    def test_incremental_job_supports_pg_to_oracle_direction(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            capture = tmp_path / "incremental_args.txt"
            fake_python = _fake_python(tmp_path, capture)
            env = _job_env(tmp_path, fake_python)

            result = subprocess.run(
                [
                    "bash",
                    str(ROOT / "jobs/incremental.sh"),
                    "pg_to_oracle",
                    "--tables",
                    "public.address",
                    "--mode",
                    "upsert",
                ],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            args = capture.read_text(encoding="utf-8")
            self.assertIn("--profile every_5min", args)
            self.assertIn("--direction postgres-to-oracle", args)
            self.assertIn("--mode upsert", args)
            self.assertIn(f"--lock-file {tmp_path / 'locks/every_5min_pg_to_oracle.lock'}", args)

    def test_legacy_every_5min_wrapper_delegates_to_incremental(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            capture = tmp_path / "legacy_args.txt"
            fake_python = _fake_python(tmp_path, capture)
            env = _job_env(tmp_path, fake_python)

            result = subprocess.run(
                ["bash", str(ROOT / "jobs/every_5min.sh"), "oracle_to_pg"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            args = capture.read_text(encoding="utf-8")
            self.assertIn("--profile every_5min", args)
            self.assertIn("--direction oracle-to-postgres", args)

    def test_job_rejects_invalid_retry_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            capture = tmp_path / "invalid_retry_args.txt"
            fake_python = _fake_python(tmp_path, capture)
            env = _job_env(tmp_path, fake_python)
            env["RETRY"] = "abc"

            result = subprocess.run(
                ["bash", str(ROOT / "jobs/daily.sh"), "oracle_to_pg"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 2)
            self.assertIn("RETRY must be a positive integer", result.stderr)


def _fake_python(tmp_path: Path, capture: Path) -> Path:
    path = tmp_path / "fake_python.sh"
    path.write_text(
        f"""#!/usr/bin/env bash
printf '%s\\n' "$*" > "{capture}"
exit 0
""",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _job_env(tmp_path: Path, fake_python: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "CONFIG_PATH": str(tmp_path / "config.yaml"),
            "PYTHON_BIN": str(fake_python),
            "LOG_DIR": str(tmp_path / "logs"),
            "LOCK_DIR": str(tmp_path / "locks"),
            "RETRY": "1",
            "TIMEOUT_SECONDS": "30",
            "LOG_ROTATE_BYTES": "1024",
        }
    )
    return env


if __name__ == "__main__":
    unittest.main()
