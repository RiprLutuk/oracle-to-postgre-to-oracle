from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging(report_dir: Path, level: int = logging.INFO) -> logging.Logger:
    report_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("oracle_pg_sync")
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(report_dir / "sync.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
