import tempfile
import unittest
from pathlib import Path

from oracle_pg_sync.checkpoint import CheckpointStore, Chunk


class CheckpointTest(unittest.TestCase):
    def test_failed_chunk_resume_skips_successful_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
            store.create_run(run_id="run1", direction="oracle_to_postgres", source_db="ora", target_db="pg")
            first = Chunk("public.sample", "id:1:10", 1, 10, "id")
            second = Chunk("public.sample", "id:11:20", 11, 20, "id")

            for chunk in [first, second]:
                store.ensure_chunk(run_id="run1", direction="oracle_to_postgres", source_db="ora", target_db="pg", chunk=chunk)
            store.start_chunk("run1", "public.sample", first.chunk_key)
            store.finish_chunk("run1", "public.sample", first.chunk_key, status="success", rows_attempted=10, rows_success=10)
            store.start_chunk("run1", "public.sample", second.chunk_key)
            store.finish_chunk("run1", "public.sample", second.chunk_key, status="failed", error_message="boom")

            self.assertEqual(store.successful_chunks("run1", "public.sample"), {"id:1:10"})
            self.assertEqual(store.chunk_status("run1", "public.sample", second.chunk_key), "failed")

    def test_watermark_roundtrip_and_reset(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "checkpoint.sqlite3")
            store.set_watermark(
                direction="oracle_to_postgres",
                table_name="public.sample",
                strategy="updated_at",
                column_name="updated_at",
                value="2026-01-01T00:00:00",
            )

            self.assertEqual(
                store.get_watermark(
                    direction="oracle_to_postgres",
                    table_name="public.sample",
                    strategy="updated_at",
                    column_name="updated_at",
                ),
                "2026-01-01T00:00:00",
            )
            self.assertEqual(store.reset_watermark("public.sample"), 1)


if __name__ == "__main__":
    unittest.main()
