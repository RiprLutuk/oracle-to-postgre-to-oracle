from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any

from oracle_pg_sync.config import AppConfig
from oracle_pg_sync.db import oracle, postgres


class PrefixedLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prefix = str(self.extra.get("prefix") or "").strip()
        if not prefix:
            return msg, kwargs
        return f"{prefix} {msg}", kwargs


class OracleWorkerPool:
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._lock = threading.Lock()
        self._connections: dict[int, Any] = {}

    @contextmanager
    def connection(self):
        ident = threading.get_ident()
        with self._lock:
            con = self._connections.get(ident)
        if con is None:
            con = _connect_with_retry(
                lambda: oracle.connect(self._config.oracle),
                logger=self._logger,
                label="Oracle connection",
            )
            with self._lock:
                self._connections[ident] = con
        yield con

    def close(self) -> None:
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
        for con in connections:
            try:
                con.close()
            except Exception:
                self._logger.debug("Oracle worker connection close failed", exc_info=True)


class BaseExecutionContext:
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.workers = 1
        self.max_db_connections = 1
        self.parallel_tables = False
        self.parallel_chunks = False
        self._thread_lock = threading.Lock()
        self._thread_labels: dict[int, str] = {}

    def close(self) -> None:
        return None

    def worker_label(self) -> str:
        ident = threading.get_ident()
        with self._thread_lock:
            label = self._thread_labels.get(ident)
            if label is None:
                label = f"Worker-{len(self._thread_labels) + 1}"
                self._thread_labels[ident] = label
        return label

    def table_logger(self, base_logger: logging.Logger, table_name: str) -> logging.LoggerAdapter:
        return PrefixedLoggerAdapter(base_logger, {"prefix": f"[{self.worker_label()}][{table_name}]"})

    def allow_table_parallelism(self, table_count: int) -> bool:
        return False

    def allow_chunk_parallelism(self, *, mode: str, table_count: int, chunk_count: int) -> bool:
        return False


class SyncExecutionContext(BaseExecutionContext):
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        super().__init__(config, logger)
        self.workers = max(1, int(config.sync.workers or 1))
        max_db_connections = config.sync.max_db_connections
        self.max_db_connections = max(1, int(max_db_connections or self.workers))
        self.parallel_tables = bool(config.sync.parallel_tables and self.workers > 1)
        self.parallel_chunks = bool(config.sync.parallel_chunks and self.workers > 1)
        self._oracle_pool = OracleWorkerPool(config, logger)
        self._postgres_pool = postgres.connection_pool(
            config.postgres,
            min_size=1,
            max_size=self.max_db_connections,
            timeout=30,
        )

    def close(self) -> None:
        try:
            self._oracle_pool.close()
        finally:
            self._postgres_pool.close()

    @contextmanager
    def oracle_connection(self):
        with self._oracle_pool.connection() as con:
            yield con

    @contextmanager
    def postgres_connection(self):
        try:
            with self._postgres_pool.connection() as con:
                yield con
        except Exception as exc:
            if _is_pool_timeout(exc):
                self.logger.warning("PostgreSQL pool wait exceeded max_db_connections=%s", self.max_db_connections)
            raise

    def allow_table_parallelism(self, table_count: int) -> bool:
        if not self.parallel_tables or table_count <= 1:
            return False
        if self.config.sync.respect_dependencies:
            self.logger.info("Dependency ordering enabled; table-level parallelism reduced to configured table order")
            return False
        return True

    def allow_chunk_parallelism(self, *, mode: str, table_count: int, chunk_count: int) -> bool:
        if not self.parallel_chunks or chunk_count <= 1:
            return False
        if mode not in {"append", "incremental_safe"}:
            return False
        if self.parallel_tables and table_count > 1:
            self.logger.warning(
                "Chunk parallelism disabled while multiple tables are running in parallel; use one parallel dimension per run"
            )
            return False
        return True


class DirectSyncExecutionContext(BaseExecutionContext):
    @contextmanager
    def oracle_connection(self):
        with oracle.connect(self.config.oracle) as con:
            yield con

    @contextmanager
    def postgres_connection(self):
        with postgres.connect(self.config.postgres) as con:
            yield con


def create_sync_execution_context(config: AppConfig, logger: logging.Logger) -> BaseExecutionContext:
    wants_parallel = bool(
        int(config.sync.workers or 1) > 1
        or config.sync.parallel_tables
        or config.sync.parallel_chunks
        or int(config.sync.max_db_connections or 1) > 1
    )
    if not wants_parallel:
        return DirectSyncExecutionContext(config, logger)
    try:
        return SyncExecutionContext(config, logger)
    except RuntimeError as exc:
        if "psycopg_pool is required" not in str(exc):
            raise
        logger.warning(
            "psycopg_pool is not installed; falling back to direct connections with workers=1 and parallel disabled"
        )
        config.sync.workers = 1
        config.sync.parallel_workers = 1
        config.sync.parallel_tables = False
        config.sync.parallel_chunks = False
        return DirectSyncExecutionContext(config, logger)


def _connect_with_retry(factory, *, logger: logging.Logger, label: str):
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, 1, 2, 4), start=1):
        try:
            return factory()
        except Exception as exc:
            last_error = exc
            if attempt >= 4:
                break
            logger.warning("%s failed attempt=%s retry_in=%ss error=%s", label, attempt, delay or 1, exc)
            time.sleep(delay or 1)
    raise RuntimeError(f"{label} retry exhausted") from last_error


def _is_pool_timeout(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc).lower()
    return "pooltimeout" in name or "couldn't get a connection" in message or "timeout" in message
