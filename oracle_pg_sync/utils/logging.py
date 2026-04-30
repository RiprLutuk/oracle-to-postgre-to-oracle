from __future__ import annotations

import logging
import sys
from pathlib import Path


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"


def setup_logging(report_dir: Path, level: int = logging.INFO) -> logging.Logger:
    report_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("oracle_pg_sync")
    logger.setLevel(level)
    for handler in list(logger.handlers):
        handler.close()
    logger.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler(report_dir / "sync.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def attach_run_log(logger: logging.Logger, run_dir: Path) -> logging.Handler:
    run_dir.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(run_dir / "logs.txt", encoding="utf-8")
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)
    return handler


def detach_log_handler(logger: logging.Logger, handler: logging.Handler | None) -> None:
    if handler is None:
        return
    logger.removeHandler(handler)
    handler.close()
