from __future__ import annotations

import logging
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from psycopg import sql

from oracle_pg_sync.checkpoint import CheckpointStore, Chunk, RollbackAction, new_run_id
from oracle_pg_sync.config import AppConfig, TableConfig
from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.lob import apply_lob_mapping_policy, lob_summary_to_fields
from oracle_pg_sync.metadata.compare import compare_table_metadata, inventory_has_fatal_mismatch
from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata
from oracle_pg_sync.sync.copy_loader import CopyMetrics, copy_rows
from oracle_pg_sync.sync.runtime import DirectSyncExecutionContext, SyncExecutionContext, create_sync_execution_context
from oracle_pg_sync.sync.staging import atomic_swap, create_backup_table, create_staging_like, drop_table
from oracle_pg_sync.utils.naming import oracle_name, split_schema_table
from oracle_pg_sync.validation import checksum_columns, checksum_result_row, stable_cursor_hash


@dataclass
class PendingWatermark:
    direction: str
    table_name: str
    strategy: str
    column_name: str
    value: Any


@dataclass
class SafeLoadResult:
    rows_loaded: int
    staging_schema: str = ""
    staging_table: str = ""
    backup_table: str = ""
    metrics: CopyMetrics = field(default_factory=CopyMetrics)


@dataclass
class SyncResult:
    table_name: str
    mode: str
    status: str
    rows_loaded: int = 0
    source_schema: str = ""
    source_table: str = ""
    target_schema: str = ""
    target_table: str = ""
    effective_where: str = ""
    oracle_count_sql_summary: str = ""
    postgres_count_sql_summary: str = ""
    rows_read_from_oracle: int = 0
    rows_written_to_postgres: int = 0
    rows_failed: int = 0
    row_count_diff: int | None = None
    validation_status: str = ""
    data_integrity_status: str = "UNKNOWN"
    failed_row_samples: list[dict[str, Any]] = field(default_factory=list)
    missing_key_report_files: str = ""
    lob_copy_status: str = ""
    lob_columns_included: str = ""
    lob_columns_excluded_from_checksum: str = ""
    oracle_row_count: int | None = None
    postgres_row_count: int | None = None
    row_count_match: bool | None = None
    dry_run: bool = True
    message: str = ""
    elapsed_seconds: float = 0.0
    run_id: str = ""
    checksum_status: str = ""
    checksum_source_hash: str = ""
    checksum_target_hash: str = ""
    checksum_source_rows: int | None = None
    checksum_target_rows: int | None = None
    checksum_rows: list[dict[str, Any]] = field(default_factory=list)
    lob_columns_detected: str = ""
    lob_columns_synced: str = ""
    lob_strategy_applied: str = ""
    lob_columns_skipped: str = ""
    lob_columns_nullified: str = ""
    lob_type: str = ""
    lob_target_type: str = ""
    lob_validation_mode: str = ""
    safe_mode: str = ""
    validation_stage: str = ""
    staging_table: str = ""
    backup_table: str = ""
    rollback_action: str = ""
    rollback_available: bool = False
    bytes_processed: int = 0
    bytes_per_second: float | None = None
    rows_per_second: float | None = None
    lob_bytes_processed: int = 0
    retry_attempts: int = 0
    watermark_candidate: PendingWatermark | None = None
    failed_tables: list[str] = field(default_factory=list)
    worker_name: str = ""

    def as_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "table_name": self.table_name,
            "direction": "oracle-to-postgres",
            "mode": self.mode,
            "status": self.status,
            "rows_loaded": self.rows_loaded,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "effective_where": self.effective_where,
            "oracle_count_sql_summary": self.oracle_count_sql_summary,
            "postgres_count_sql_summary": self.postgres_count_sql_summary,
            "rows_read_from_oracle": self.rows_read_from_oracle,
            "rows_written_to_postgres": self.rows_written_to_postgres,
            "rows_failed": self.rows_failed,
            "oracle_row_count": self.oracle_row_count,
            "postgres_row_count": self.postgres_row_count,
            "row_count_match": self.row_count_match,
            "row_count_diff": self.row_count_diff,
            "validation_status": self.validation_status,
            "data_integrity_status": self.data_integrity_status,
            "failed_row_samples": json.dumps(self.failed_row_samples, ensure_ascii=True) if self.failed_row_samples else "",
            "missing_key_report_files": self.missing_key_report_files,
            "lob_copy_status": self.lob_copy_status,
            "lob_columns_included": self.lob_columns_included,
            "lob_columns_excluded_from_checksum": self.lob_columns_excluded_from_checksum,
            "dry_run": self.dry_run,
            "message": self.message,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "checksum_status": self.checksum_status,
            "checksum_source_hash": self.checksum_source_hash,
            "checksum_target_hash": self.checksum_target_hash,
            "checksum_source_rows": self.checksum_source_rows,
            "checksum_target_rows": self.checksum_target_rows,
            "lob_columns_detected": self.lob_columns_detected,
            "lob_columns_synced": self.lob_columns_synced,
            "lob_strategy_applied": self.lob_strategy_applied,
            "lob_columns_skipped": self.lob_columns_skipped,
            "lob_columns_nullified": self.lob_columns_nullified,
            "lob_type": self.lob_type,
            "lob_target_type": self.lob_target_type,
            "lob_validation_mode": self.lob_validation_mode,
            "safe_mode": self.safe_mode,
            "validation_stage": self.validation_stage,
            "staging_table": self.staging_table,
            "backup_table": self.backup_table,
            "rollback_action": self.rollback_action,
            "rollback_available": self.rollback_available,
            "bytes_processed": self.bytes_processed,
            "bytes_per_second": self.bytes_per_second,
            "rows_per_second": self.rows_per_second,
            "lob_bytes_processed": self.lob_bytes_processed,
            "retry_attempts": self.retry_attempts,
            "worker_name": self.worker_name,
        }


class OracleToPostgresSync:
    def __init__(self, config: AppConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger("oracle_pg_sync")

    def sync_tables(
        self,
        tables: list[str],
        *,
        mode_override: str | None = None,
        execute: bool = False,
        force: bool = False,
        checkpoint_store: CheckpointStore | None = None,
        run_id: str | None = None,
        resume: bool = False,
        incremental: bool = False,
        full_refresh: bool = False,
    ) -> list[SyncResult]:
        run_id = run_id or new_run_id()
        if checkpoint_store:
            checkpoint_store.create_run(
                run_id=run_id,
                direction="oracle_to_postgres",
                source_db=self.config.oracle.schema,
                target_db=self.config.postgres.schema,
                job_name=self.config.job.name,
                mode=mode_override or self.config.sync.default_mode,
            )
        execution_context = create_sync_execution_context(self.config, self.logger)
        table_order = {
            self.config.resolve_table_name(name, strict=False): index
            for index, name in enumerate(tables)
        }
        results: list[SyncResult] = []
        try:
            if execution_context.allow_table_parallelism(len(tables)):
                worker_count = min(execution_context.workers, len(tables))
                self.logger.info(
                    "Parallel sync enabled dimension=tables workers=%s max_db_connections=%s",
                    worker_count,
                    execution_context.max_db_connections,
                )
                with ThreadPoolExecutor(max_workers=worker_count) as pool:
                    futures = {
                        pool.submit(
                            self._sync_table_task,
                            table,
                            mode_override=mode_override,
                            execute=execute,
                            force=force,
                            checkpoint_store=checkpoint_store,
                            run_id=run_id,
                            resume=resume,
                            incremental=incremental,
                            full_refresh=full_refresh,
                            execution_context=execution_context,
                            total_tables=len(tables),
                        ): table
                        for table in tables
                    }
                    for future in as_completed(futures):
                        results.append(future.result())
            else:
                for table in tables:
                    results.append(
                        self._sync_table_task(
                            table,
                            mode_override=mode_override,
                            execute=execute,
                            force=force,
                            checkpoint_store=checkpoint_store,
                            run_id=run_id,
                            resume=resume,
                            incremental=incremental,
                            full_refresh=full_refresh,
                            execution_context=execution_context,
                            total_tables=len(tables),
                        )
                    )
        finally:
            execution_context.close()
        results = sorted(results, key=lambda r: table_order.get(r.table_name, len(table_order)))
        if checkpoint_store:
            checkpoint_store.finish_run(
                run_id,
                status="failed" if any(result.status == "FAILED" for result in results) else "success",
            )
        return results

    def _sync_table_task(
        self,
        table_name: str,
        *,
        mode_override: str | None = None,
        execute: bool = False,
        force: bool = False,
        checkpoint_store: CheckpointStore | None = None,
        run_id: str | None = None,
        resume: bool = False,
        incremental: bool = False,
        full_refresh: bool = False,
        execution_context: SyncExecutionContext,
        total_tables: int,
    ) -> SyncResult:
        worker_logger = execution_context.table_logger(
            self.logger,
            split_schema_table(table_name, self.config.postgres.schema).fqname,
        )
        worker_sync = OracleToPostgresSync(self.config, worker_logger)
        return worker_sync.sync_table(
            table_name,
            mode_override=mode_override,
            execute=execute,
            force=force,
            checkpoint_store=checkpoint_store,
            run_id=run_id,
            resume=resume,
            incremental=incremental,
            full_refresh=full_refresh,
            execution_context=execution_context,
            total_tables=total_tables,
        )

    def sync_table(
        self,
        table_name: str,
        *,
        mode_override: str | None = None,
        execute: bool = False,
        force: bool = False,
        checkpoint_store: CheckpointStore | None = None,
        run_id: str | None = None,
        resume: bool = False,
        incremental: bool = False,
        full_refresh: bool = False,
        execution_context: SyncExecutionContext | None = None,
        total_tables: int = 1,
    ) -> SyncResult:
        if execution_context is None:
            execution_context = DirectSyncExecutionContext(self.config, self.logger)
            try:
                return self.sync_table(
                    table_name,
                    mode_override=mode_override,
                    execute=execute,
                    force=force,
                    checkpoint_store=checkpoint_store,
                    run_id=run_id,
                    resume=resume,
                    incremental=incremental,
                    full_refresh=full_refresh,
                    execution_context=execution_context,
                    total_tables=total_tables,
                )
            finally:
                execution_context.close()
        started = time.time()
        table_cfg = self.config.table_config(table_name) or TableConfig(name=table_name)
        target_schema = table_cfg.target_schema or split_schema_table(table_cfg.name, self.config.postgres.schema).schema
        target_table = table_cfg.target_table or split_schema_table(table_cfg.name, self.config.postgres.schema).table
        table = split_schema_table(f"{target_schema}.{target_table}", self.config.postgres.schema)
        mode = (
            mode_override
            or table_cfg.oracle_to_postgres_mode
            or table_cfg.mode
            or self.config.sync.default_mode
        ).lower()
        mode = self._normalize_mode(mode, incremental=incremental or table_cfg.incremental.enabled)
        dry_run = not execute or self.config.sync.dry_run and not execute

        run_id = run_id or new_run_id()
        result = SyncResult(table.fqname, mode, "PENDING", dry_run=dry_run, run_id=run_id)
        result.worker_name = execution_context.worker_label()
        result.safe_mode = mode
        result.source_schema = table_cfg.source_schema or self.config.oracle.schema
        result.source_table = table_cfg.source_table or table.table
        result.target_schema = table.schema
        result.target_table = table.table
        self.logger.info(
            "Resolved %s -> %s.%s to %s.%s mode=%s where=%s",
            table_name,
            result.source_schema,
            result.source_table,
            result.target_schema,
            result.target_table,
            mode,
            table_cfg.where or "",
        )
        self.logger.info("Sync %s mode=%s dry_run=%s", table.fqname, mode, dry_run)

        try:
            with execution_context.oracle_connection() as ocon, execution_context.postgres_connection() as pcon:
                with ocon.cursor() as ocur, pcon.cursor() as pcur:
                    owner = result.source_schema
                    source_table = result.source_table
                    oracle_meta = fetch_oracle_metadata(
                        ocur,
                        owner=owner,
                        table=source_table,
                        fast_count=self.config.sync.fast_count,
                    )
                    pg_meta = fetch_pg_metadata(
                        pcur,
                        schema=table.schema,
                        table=table.table,
                        fast_count=self.config.sync.fast_count,
                    )
                    inventory, _, _ = compare_table_metadata(
                        table_name=table.fqname,
                        config=self.config,
                        oracle_meta=oracle_meta,
                        postgres_meta=pg_meta,
                    )
                    if inventory_has_fatal_mismatch(inventory) and not force:
                        result.status = "SKIPPED"
                        result.message = "struktur/table mismatch; gunakan --force jika tetap ingin sync"
                        return result

                    mapping = self._column_mapping(table.fqname, pg_meta.columns, oracle_meta.columns)
                    if not mapping:
                        result.status = "SKIPPED"
                        result.message = "tidak ada kolom yang bisa dimapping"
                        return result
                    mapping, lob_summary = apply_lob_mapping_policy(
                        mapping,
                        config=self.config,
                        table_cfg=table_cfg,
                        table_name=table.fqname,
                        source_columns=oracle_meta.columns,
                    )
                    self._apply_lob_summary(result, lob_summary)
                    if not mapping:
                        result.status = "SKIPPED"
                        result.message = "semua kolom termapping di-skip oleh lob_strategy"
                        return result

                    pg_columns = [pg_col for pg_col, _ in mapping]
                    swap_size = self._swap_size_estimate(pcur, table.schema, table.table) if mode in {"swap", "swap_safe"} else None
                    base_where = table_cfg.where
                    incremental_where = self._incremental_where(
                        checkpoint_store,
                        table_cfg,
                        table.fqname,
                        incremental=incremental,
                        full_refresh=full_refresh,
                    )
                    effective_where = _combine_where(base_where, incremental_where)
                    result.effective_where = effective_where or ""
                    result.oracle_count_sql_summary = _oracle_count_sql_summary(owner, source_table, effective_where)
                    result.postgres_count_sql_summary = _postgres_count_sql_summary(table.schema, table.table)
                    if dry_run:
                        result.status = "DRY_RUN"
                        result.message = self._dry_run_message(table.fqname, mode, len(pg_columns), swap_size)
                        if incremental_where:
                            result.message += f"; incremental filter: {incremental_where}"
                        return result

                    precheck_result = self._precheck_skip_if_rowcount_match(
                        result,
                        table_cfg,
                        mode,
                        incremental_where,
                        full_refresh=full_refresh,
                        ocur=ocur,
                        pcur=pcur,
                        owner=owner,
                        source_table=source_table,
                        target_schema=table.schema,
                        target_table=table.table,
                        where=effective_where,
                    )
                    if precheck_result is not None:
                        if checkpoint_store:
                            checkpoint_store.record_event(
                                run_id=run_id,
                                table_name=table.fqname,
                                phase="precheck_rowcount",
                                status="skipped",
                                message=precheck_result.message,
                                details={
                                    "oracle_row_count": precheck_result.oracle_row_count,
                                    "postgres_row_count": precheck_result.postgres_row_count,
                                },
                            )
                        return precheck_result

                    chunks = self._plan_chunks(ocur, owner, source_table, table_cfg, effective_where)
                    successful = checkpoint_store.successful_chunks(run_id, table.fqname) if checkpoint_store and resume else set()
                    load_result = SafeLoadResult(rows_loaded=0)
                    result.validation_stage = "planned"
                    if checkpoint_store:
                        checkpoint_store.record_event(
                            run_id=run_id,
                            table_name=table.fqname,
                            phase="load_started",
                            status="running",
                            details={"mode": mode},
                        )

                    if mode == "truncate":
                        load_result.metrics = CopyMetrics()
                        rows_loaded = self._sync_truncate(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            table.table,
                            mapping,
                            effective_where,
                            metrics=load_result.metrics,
                            table_name=table.fqname,
                            key_columns=table_cfg.key_columns,
                        )
                        load_result.rows_loaded = rows_loaded
                        result.validation_stage = "direct_truncate_loaded"

                    elif mode == "truncate_safe":
                        load_result = self._sync_truncate_safe(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            source_table,
                            table.table,
                            mapping,
                            chunks,
                            checkpoint_store=checkpoint_store,
                            run_id=run_id,
                            execution_context=execution_context,
                            total_tables=total_tables,
                        )
                        result.validation_stage = "staging_loaded"
                        result.staging_table = f"{load_result.staging_schema}.{load_result.staging_table}"
                        checksum_rows = self._validate_checksum(
                            ocur,
                            pcur,
                            owner,
                            load_result.staging_schema,
                            source_table,
                            table.fqname,
                            oracle_meta.columns,
                            pg_meta.columns,
                            effective_where,
                            mapping,
                            target_table=load_result.staging_table,
                            force_enabled=True,
                        )
                        result.checksum_rows = checksum_rows
                        if checksum_rows:
                            _apply_checksum_summary(result, checksum_rows)
                        result.oracle_row_count, result.postgres_row_count, result.row_count_match = self._safe_rowcount_validation(
                            ocur,
                            pcur,
                            owner,
                            source_table,
                            load_result.staging_schema,
                            load_result.staging_table,
                            effective_where,
                        )
                        if result.checksum_status == "MISMATCH" or result.row_count_match is False:
                            raise RuntimeError("staging validation failed before truncate_safe cutover")
                        backup_table = self._apply_truncate_from_staging(
                            pcur,
                            schema=table.schema,
                            target_table=table.table,
                            staging_schema=load_result.staging_schema,
                            staging_table=load_result.staging_table,
                            columns=pg_columns,
                            backup_before_truncate=self.config.sync.backup_before_truncate,
                            run_id=run_id,
                        )
                        self._register_rollback_action(
                            checkpoint_store,
                            result,
                            run_id=run_id,
                            table_name=table.fqname,
                            action_type="truncate_safe",
                            target_schema=table.schema,
                            target_table=table.table,
                            backup_schema=table.schema,
                            backup_table=backup_table,
                            staging_schema=load_result.staging_schema,
                            staging_table=load_result.staging_table,
                        )
                    elif mode in {"swap", "swap_safe"}:
                        guard_message = self._swap_guard_message(table.fqname, swap_size, force=force)
                        if guard_message:
                            result.status = "SKIPPED"
                            result.message = guard_message
                            return result
                        self._log_swap_risk(table.fqname, swap_size)
                        load_result = self._sync_swap_safe(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            source_table,
                            table.table,
                            mapping,
                            effective_where,
                            run_id=run_id,
                        )
                        result.validation_stage = "staging_loaded"
                        result.staging_table = f"{load_result.staging_schema}.{load_result.staging_table}"
                        checksum_rows = self._validate_checksum(
                            ocur,
                            pcur,
                            owner,
                            load_result.staging_schema,
                            source_table,
                            table.fqname,
                            oracle_meta.columns,
                            pg_meta.columns,
                            effective_where,
                            mapping,
                            target_table=load_result.staging_table,
                            force_enabled=True,
                        )
                        result.checksum_rows = checksum_rows
                        if checksum_rows:
                            _apply_checksum_summary(result, checksum_rows)
                        result.oracle_row_count, result.postgres_row_count, result.row_count_match = self._safe_rowcount_validation(
                            ocur,
                            pcur,
                            owner,
                            source_table,
                            load_result.staging_schema,
                            load_result.staging_table,
                            effective_where,
                        )
                        if result.checksum_status == "MISMATCH" or result.row_count_match is False:
                            raise RuntimeError("staging validation failed before swap_safe cutover")
                        backup_table = self._apply_swap_from_staging(
                            pcur,
                            schema=table.schema,
                            table=table.table,
                            staging_table=load_result.staging_table,
                        )
                        self._register_rollback_action(
                            checkpoint_store,
                            result,
                            run_id=run_id,
                            table_name=table.fqname,
                            action_type="swap_safe",
                            target_schema=table.schema,
                            target_table=table.table,
                            backup_schema=table.schema,
                            backup_table=backup_table,
                            staging_schema=load_result.staging_schema,
                            staging_table=load_result.staging_table,
                        )
                    elif mode == "incremental_safe":
                        load_result = self._sync_incremental_safe(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            source_table,
                            table.table,
                            mapping,
                            chunks,
                            table_cfg.key_columns,
                            effective_where,
                            run_id=run_id,
                            checkpoint_store=checkpoint_store,
                            execution_context=execution_context,
                            total_tables=total_tables,
                        )
                        result.validation_stage = "staging_loaded"
                        result.staging_table = f"{load_result.staging_schema}.{load_result.staging_table}"
                        checksum_rows = self._validate_checksum(
                            ocur,
                            pcur,
                            owner,
                            load_result.staging_schema,
                            source_table,
                            table.fqname,
                            oracle_meta.columns,
                            pg_meta.columns,
                            effective_where,
                            mapping,
                            target_table=load_result.staging_table,
                            force_enabled=True,
                        )
                        result.checksum_rows = checksum_rows
                        if checksum_rows:
                            _apply_checksum_summary(result, checksum_rows)
                        result.oracle_row_count, result.postgres_row_count, result.row_count_match = self._safe_rowcount_validation(
                            ocur,
                            pcur,
                            owner,
                            source_table,
                            load_result.staging_schema,
                            load_result.staging_table,
                            effective_where,
                        )
                        if result.checksum_status == "MISMATCH" or result.row_count_match is False:
                            raise RuntimeError("staging validation failed before incremental_safe apply")
                        backup_table = self._apply_incremental_from_staging(
                            pcur,
                            schema=table.schema,
                            target_table=table.table,
                            staging_schema=load_result.staging_schema,
                            staging_table=load_result.staging_table,
                            columns=pg_columns,
                            key_columns=table_cfg.key_columns,
                            run_id=run_id,
                        )
                        self._register_rollback_action(
                            checkpoint_store,
                            result,
                            run_id=run_id,
                            table_name=table.fqname,
                            action_type="incremental_safe",
                            target_schema=table.schema,
                            target_table=table.table,
                            backup_schema=table.schema,
                            backup_table=backup_table,
                            staging_schema=load_result.staging_schema,
                            staging_table=load_result.staging_table,
                        )
                    elif mode == "append":
                        load_result.metrics = CopyMetrics()
                        load_result.rows_loaded = self._copy_chunks(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            source_table,
                            table.table,
                            mapping,
                            chunks,
                            checkpoint_store=checkpoint_store,
                            run_id=run_id,
                            resume_successful=successful,
                            metrics=load_result.metrics,
                            execution_context=execution_context,
                            mode=mode,
                            total_tables=total_tables,
                        )
                        checksum_rows = self._validate_checksum(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            source_table,
                            table.fqname,
                            oracle_meta.columns,
                            pg_meta.columns,
                            effective_where,
                            mapping,
                        )
                        result.checksum_rows = checksum_rows
                        if checksum_rows:
                            _apply_checksum_summary(result, checksum_rows)
                    else:
                        raise ValueError(f"Unsupported sync mode: {mode}")

                    result.rows_loaded = load_result.rows_loaded
                    self._apply_copy_metrics(result, load_result.metrics)
                    self._validate_copy_completeness(result)
                    if result.checksum_status == "MISMATCH":
                        raise RuntimeError("checksum mismatch after load")
                    if self._rowcount_validation_enabled(table_cfg):
                        result.oracle_row_count, result.postgres_row_count, result.row_count_match = self._safe_rowcount_validation(
                            ocur,
                            pcur,
                            owner,
                            source_table,
                            table.schema,
                            table.table,
                            effective_where,
                        )
                        result.row_count_diff = (result.postgres_row_count or 0) - (result.oracle_row_count or 0)
                        result.validation_status = "validation_pass" if result.row_count_match else "validation_failed"
                        if result.row_count_match is False:
                            message = (
                                f"rowcount mismatch source={result.oracle_row_count} "
                                f"target={result.postgres_row_count} diff={result.row_count_diff}"
                            )
                            if self._rowcount_fail_on_mismatch(table_cfg):
                                raise RuntimeError(message)
                            result.status = "WARNING"
                            result.message = message
                    else:
                        result.validation_status = "validation_skipped"
                    result.data_integrity_status = self._data_integrity_status(result)
                    if result.data_integrity_status == "FAIL":
                        raise RuntimeError(self._data_integrity_failure_message(result))
                    if result.data_integrity_status == "UNKNOWN":
                        result.status = "WARNING"
                        result.message = result.message or "data integrity validation incomplete"
                    elif result.status != "WARNING":
                        result.status = "SUCCESS"
                    if self.config.sync.analyze_after_load and mode in {"truncate", "truncate_safe", "swap_safe", "incremental_safe"}:
                        postgres.analyze_table(pcur, table.schema, table.table)
                    result.watermark_candidate = self._build_watermark_candidate(
                        table_cfg,
                        table.fqname,
                        ocur,
                        owner,
                        source_table,
                        effective_where,
                        enabled=incremental or table_cfg.incremental.enabled,
                    )
                    pcon.commit()
                    if checkpoint_store:
                        checkpoint_store.record_event(
                            run_id=run_id,
                            table_name=table.fqname,
                            phase="table_committed",
                            status="success",
                            details={"mode": mode, "rows_loaded": result.rows_loaded},
                        )
                    return result
        except Exception as exc:
            try:
                if "pcon" in locals():
                    pcon.rollback()
            except Exception:
                pass
            result.status = "FAILED"
            result.message = str(exc)
            result.data_integrity_status = "FAIL"
            result.failed_tables = [table.fqname]
            if checkpoint_store:
                checkpoint_store.record_event(
                    run_id=run_id,
                    table_name=table.fqname,
                    phase="table_failed",
                    status="failed",
                    message=str(exc),
                )
            self.logger.exception("Sync failed for %s", table.fqname)
            return result
        finally:
            result.elapsed_seconds = time.time() - started
            self._finalize_metrics(result, load_result.metrics if "load_result" in locals() else CopyMetrics())

    def _sync_truncate(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        table: str,
        mapping: list[tuple[str, str | None]],
        where: str | None,
        *,
        metrics: CopyMetrics | None = None,
        table_name: str = "",
        key_columns: list[str] | None = None,
    ) -> int:
        postgres.set_local_timeouts(
            pcur,
            lock_timeout=self.config.sync.pg_lock_timeout,
            statement_timeout=self.config.sync.pg_statement_timeout,
        )
        postgres.truncate_table(pcur, schema, table, cascade=self.config.sync.truncate_cascade)
        return self._copy_oracle_to_pg(
            ocur,
            pcur,
            owner,
            schema,
            table,
            table,
            mapping,
            where,
            metrics=metrics,
            table_name=table_name,
            key_columns=key_columns,
        )

    def _sync_truncate_safe(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        chunks: list[Chunk],
        *,
        checkpoint_store: CheckpointStore | None,
        run_id: str,
        execution_context: SyncExecutionContext,
        total_tables: int,
    ) -> SafeLoadResult:
        postgres.set_local_timeouts(
            pcur,
            lock_timeout=self.config.sync.pg_lock_timeout,
            statement_timeout=self.config.sync.pg_statement_timeout,
        )
        metrics = CopyMetrics()
        staging_schema, staging = create_staging_like(
            pcur,
            schema,
            target_table,
            run_id=run_id,
            staging_schema=self.config.sync.staging_schema,
        )
        self._cleanup_staging_retention(pcur, staging_schema, target_table)
        rows = self._copy_chunks(
            ocur,
            pcur,
            owner,
            staging_schema,
            source_table,
            staging,
            mapping,
            chunks,
            checkpoint_store=checkpoint_store,
            run_id=run_id,
            resume_successful=set(),
            metrics=metrics,
            execution_context=execution_context,
            mode="truncate_safe",
            total_tables=total_tables,
        )
        if self.config.sync.analyze_after_load:
            postgres.analyze_table(pcur, staging_schema, staging)
        return SafeLoadResult(
            rows_loaded=rows,
            staging_schema=staging_schema,
            staging_table=staging,
            metrics=metrics,
        )

    def _sync_swap_safe(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        where: str | None,
        *,
        run_id: str,
    ) -> SafeLoadResult:
        postgres.set_local_timeouts(
            pcur,
            lock_timeout=self.config.sync.pg_lock_timeout,
            statement_timeout=self.config.sync.pg_statement_timeout,
        )
        metrics = CopyMetrics()
        staging_schema, staging = create_staging_like(pcur, schema, target_table, run_id=run_id)
        self._cleanup_staging_retention(pcur, staging_schema, target_table)
        rows = self._copy_oracle_to_pg(
            ocur,
            pcur,
            owner,
            staging_schema,
            source_table,
            staging,
            mapping,
            where,
            metrics=metrics,
        )
        if self.config.sync.analyze_after_load:
            postgres.analyze_table(pcur, staging_schema, staging)
        return SafeLoadResult(
            rows_loaded=rows,
            staging_schema=staging_schema,
            staging_table=staging,
            metrics=metrics,
        )

    def _sync_incremental_safe(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        chunks: list[Chunk],
        key_columns: list[str],
        where: str | None,
        *,
        run_id: str,
        checkpoint_store: CheckpointStore | None,
        execution_context: SyncExecutionContext,
        total_tables: int,
    ) -> SafeLoadResult:
        if not key_columns:
            raise ValueError(f"Mode upsert but key_columns is empty for {schema}.{target_table}")
        metrics = CopyMetrics()
        staging_schema, staging = create_staging_like(pcur, schema, target_table, run_id=run_id)
        self._cleanup_staging_retention(pcur, staging_schema, target_table)
        if execution_context.allow_chunk_parallelism(
            mode="incremental_safe",
            table_count=total_tables,
            chunk_count=len(chunks),
        ):
            pcur.connection.commit()
            rows = self._copy_chunks(
                ocur,
                pcur,
                owner,
                staging_schema,
                source_table,
                staging,
                mapping,
                chunks,
                checkpoint_store=checkpoint_store,
                run_id=run_id,
                resume_successful=set(),
                metrics=metrics,
                execution_context=execution_context,
                mode="incremental_safe",
                total_tables=total_tables,
            )
        else:
            rows = self._copy_oracle_to_pg(
                ocur,
                pcur,
                owner,
                staging_schema,
                source_table,
                staging,
                mapping,
                where,
                metrics=metrics,
            )
        pg_columns = [pg_col for pg_col, _ in mapping]
        key_set = {k.lower() for k in key_columns}
        update_columns = [c for c in pg_columns if c.lower() not in key_set]
        conflict_action = (
            sql.SQL("DO UPDATE SET {}").format(sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in update_columns
            ))
            if update_columns
            else sql.SQL("DO NOTHING")
        )
        insert_stmt = sql.SQL(
            "INSERT INTO {} ({cols}) SELECT {cols} FROM {} "
            "ON CONFLICT ({keys}) {}"
        ).format(
            table_ident(schema, target_table),
            table_ident(staging_schema, staging),
            conflict_action,
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in pg_columns),
            keys=sql.SQL(", ").join(sql.Identifier(c.lower()) for c in key_columns),
        )
        return SafeLoadResult(
            rows_loaded=rows,
            staging_schema=staging_schema,
            staging_table=staging,
            metrics=metrics,
        )

    def _apply_truncate_from_staging(
        self,
        pcur,
        *,
        schema: str,
        target_table: str,
        staging_schema: str,
        staging_table: str,
        columns: list[str],
        backup_before_truncate: bool,
        run_id: str,
    ) -> str:
        backup_table = ""
        if backup_before_truncate:
            backup_table = create_backup_table(pcur, schema, target_table, token=run_id)
        postgres.truncate_table(pcur, schema, target_table, cascade=self.config.sync.truncate_cascade)
        postgres.insert_from_table(
            pcur,
            target_schema=schema,
            target_table=target_table,
            source_schema=staging_schema,
            source_table=staging_table,
            columns=columns,
        )
        drop_table(pcur, staging_schema, staging_table)
        self._cleanup_backup_retention(pcur, schema, target_table)
        return backup_table

    def _apply_swap_from_staging(self, pcur, *, schema: str, table: str, staging_table: str) -> str:
        backup_table = atomic_swap(pcur, schema, table, staging_table=staging_table)
        self._cleanup_backup_retention(pcur, schema, table)
        return backup_table

    def _apply_incremental_from_staging(
        self,
        pcur,
        *,
        schema: str,
        target_table: str,
        staging_schema: str,
        staging_table: str,
        columns: list[str],
        key_columns: list[str],
        run_id: str,
    ) -> str:
        backup_table = create_backup_table(pcur, schema, target_table, token=run_id)
        key_set = {k.lower() for k in key_columns}
        update_columns = [c for c in columns if c.lower() not in key_set]
        conflict_action = (
            sql.SQL("DO UPDATE SET {}").format(
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in update_columns
                )
            )
            if update_columns
            else sql.SQL("DO NOTHING")
        )
        insert_stmt = sql.SQL(
            "INSERT INTO {} ({cols}) SELECT {cols} FROM {} ON CONFLICT ({keys}) {}"
        ).format(
            table_ident(schema, target_table),
            table_ident(staging_schema, staging_table),
            conflict_action,
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            keys=sql.SQL(", ").join(sql.Identifier(c.lower()) for c in key_columns),
        )
        pcur.execute(insert_stmt)
        drop_table(pcur, staging_schema, staging_table)
        self._cleanup_backup_retention(pcur, schema, target_table)
        return backup_table

    def _copy_oracle_to_pg(
        self,
        ocur,
        pcur,
        owner: str,
        source_schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        where: str | None,
        *,
        metrics: CopyMetrics | None = None,
        table_name: str = "",
        chunk_key: str = "",
        key_columns: list[str] | None = None,
    ) -> int:
        rows_cursor = oracle.select_rows(ocur, owner, source_table, mapping, where=where)
        pg_columns = [pg_col for pg_col, _ in mapping]
        return copy_rows(
            pcur,
            schema=source_schema,
            table=target_table,
            columns=pg_columns,
            rows=rows_cursor,
            lob_chunk_size_bytes=self.config.lob_strategy.lob_chunk_size_bytes,
            metrics=metrics,
            table_name=table_name or f"{source_schema}.{target_table}",
            chunk_key=chunk_key,
            key_columns=key_columns or [],
            skip_failed_rows=self.config.sync.skip_failed_rows,
            failed_row_sample_limit=self.config.sync.failed_row_sample_limit,
        )

    def _copy_chunks(
        self,
        ocur,
        pcur,
        owner: str,
        source_schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        chunks: list[Chunk],
        *,
        checkpoint_store: CheckpointStore | None,
        run_id: str,
        resume_successful: set[str],
        metrics: CopyMetrics | None = None,
        execution_context: SyncExecutionContext,
        mode: str,
        total_tables: int,
    ) -> int:
        if execution_context.allow_chunk_parallelism(
            mode=mode,
            table_count=total_tables,
            chunk_count=len(chunks),
        ):
            return self._copy_chunks_parallel(
                owner,
                source_schema,
                source_table,
                target_table,
                mapping,
                chunks,
                checkpoint_store=checkpoint_store,
                run_id=run_id,
                resume_successful=resume_successful,
                metrics=metrics,
                execution_context=execution_context,
            )
        total = 0
        for chunk in chunks:
            if checkpoint_store:
                checkpoint_store.ensure_chunk(
                    run_id=run_id,
                    direction="oracle_to_postgres",
                    source_db=self.config.oracle.schema,
                    target_db=self.config.postgres.schema,
                    chunk=chunk,
                )
            if chunk.chunk_key in resume_successful:
                self.logger.info("Skip successful checkpoint chunk %s %s", chunk.table_name, chunk.chunk_key)
                continue
            if checkpoint_store and not checkpoint_store.claim_chunk(run_id, chunk.table_name, chunk.chunk_key):
                self.logger.info(
                    "Skip claimed checkpoint chunk %s %s status=%s",
                    chunk.table_name,
                    chunk.chunk_key,
                    checkpoint_store.chunk_status(run_id, chunk.table_name, chunk.chunk_key),
                )
                continue
            try:
                rows = self._copy_oracle_to_pg(
                    ocur,
                    pcur,
                    owner,
                    source_schema,
                    source_table,
                    target_table,
                    mapping,
                    _chunk_where(chunk),
                    metrics=metrics,
                    table_name=chunk.table_name,
                    chunk_key=chunk.chunk_key,
                    key_columns=chunk.primary_key and [chunk.primary_key] or [],
                )
                total += rows
                if checkpoint_store:
                    checkpoint_store.finish_chunk(
                        run_id,
                        chunk.table_name,
                        chunk.chunk_key,
                        status="success",
                        rows_attempted=rows,
                        rows_success=rows,
                    )
            except Exception as exc:
                if checkpoint_store:
                    checkpoint_store.finish_chunk(
                        run_id,
                        chunk.table_name,
                        chunk.chunk_key,
                        status="failed",
                        error_message=str(exc),
                    )
                raise
        return total

    def _copy_chunks_parallel(
        self,
        owner: str,
        target_schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        chunks: list[Chunk],
        *,
        checkpoint_store: CheckpointStore | None,
        run_id: str,
        resume_successful: set[str],
        metrics: CopyMetrics | None,
        execution_context: SyncExecutionContext,
    ) -> int:
        total = 0
        worker_count = min(execution_context.workers, len(chunks))
        self.logger.info("Parallel chunk copy enabled chunks=%s workers=%s", len(chunks), worker_count)
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {
                pool.submit(
                    self._copy_chunk_task,
                    chunk,
                    owner=owner,
                    target_schema=target_schema,
                    source_table=source_table,
                    target_table=target_table,
                    mapping=mapping,
                    checkpoint_store=checkpoint_store,
                    run_id=run_id,
                    resume_successful=resume_successful,
                    execution_context=execution_context,
                ): chunk
                for chunk in chunks
            }
            for future in as_completed(futures):
                rows, chunk_metrics = future.result()
                total += rows
                _merge_copy_metrics(metrics, chunk_metrics)
        return total

    def _copy_chunk_task(
        self,
        chunk: Chunk,
        *,
        owner: str,
        target_schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        checkpoint_store: CheckpointStore | None,
        run_id: str,
        resume_successful: set[str],
        execution_context: SyncExecutionContext,
    ) -> tuple[int, CopyMetrics]:
        chunk_logger = execution_context.table_logger(self.logger, chunk.table_name)
        chunk_sync = OracleToPostgresSync(self.config, chunk_logger)
        if checkpoint_store:
            checkpoint_store.ensure_chunk(
                run_id=run_id,
                direction="oracle_to_postgres",
                source_db=self.config.oracle.schema,
                target_db=self.config.postgres.schema,
                chunk=chunk,
            )
        if chunk.chunk_key in resume_successful:
            chunk_logger.info("Skip successful checkpoint chunk %s %s", chunk.table_name, chunk.chunk_key)
            return 0, CopyMetrics()
        if checkpoint_store and not checkpoint_store.claim_chunk(run_id, chunk.table_name, chunk.chunk_key):
            chunk_logger.info(
                "Skip claimed checkpoint chunk %s %s status=%s",
                chunk.table_name,
                chunk.chunk_key,
                checkpoint_store.chunk_status(run_id, chunk.table_name, chunk.chunk_key),
            )
            return 0, CopyMetrics()

        metrics = CopyMetrics()
        try:
            with execution_context.oracle_connection() as ocon, execution_context.postgres_connection() as pcon:
                with ocon.cursor() as ocur, pcon.cursor() as pcur:
                    rows = chunk_sync._copy_oracle_to_pg(
                        ocur,
                        pcur,
                        owner,
                        target_schema,
                        source_table,
                        target_table,
                        mapping,
                        _chunk_where(chunk),
                        metrics=metrics,
                        table_name=chunk.table_name,
                        chunk_key=chunk.chunk_key,
                        key_columns=chunk.primary_key and [chunk.primary_key] or [],
                    )
                    pcon.commit()
            if checkpoint_store:
                checkpoint_store.finish_chunk(
                    run_id,
                    chunk.table_name,
                    chunk.chunk_key,
                    status="success",
                    rows_attempted=rows,
                    rows_success=rows,
                )
            return rows, metrics
        except Exception as exc:
            try:
                if "pcon" in locals():
                    pcon.rollback()
            except Exception:
                pass
            if checkpoint_store:
                checkpoint_store.finish_chunk(
                    run_id,
                    chunk.table_name,
                    chunk.chunk_key,
                    status="failed",
                    error_message=str(exc),
                )
            raise

    def _truncate_resume_successful_chunks(
        self,
        table_name: str,
        successful: set[str],
        *,
        resume: bool,
    ) -> set[str]:
        if not resume or not successful:
            return successful
        strategy = (self.config.sync.truncate_resume_strategy or "restart_table").lower()
        if strategy not in {"restart_table", "staging"}:
            raise ValueError(f"Unsupported truncate_resume_strategy: {strategy}")
        self.logger.warning(
            "Resume mode truncate untuk %s tidak akan skip partial chunks; strategy=%s reload full table.",
            table_name,
            strategy,
        )
        return set()

    def _mark_table_phase(
        self,
        checkpoint_store: CheckpointStore | None,
        run_id: str,
        table_name: str,
        phase: str,
        *,
        rows_attempted: int = 0,
        rows_success: int = 0,
    ) -> None:
        if not checkpoint_store:
            return
        checkpoint_store.mark_table_phase(
            run_id=run_id,
            direction="oracle_to_postgres",
            source_db=self.config.oracle.schema,
            target_db=self.config.postgres.schema,
            table_name=table_name,
            phase=phase,
            rows_attempted=rows_attempted,
            rows_success=rows_success,
        )

    def _column_mapping(
        self,
        fq_table: str,
        pg_columns: list[Any],
        oracle_columns: list[Any],
    ) -> list[tuple[str, str]]:
        rename = self.config.rename_columns.get(fq_table.lower(), {})
        pg_to_oracle = {pg_name.lower(): oracle_col.lower() for oracle_col, pg_name in rename.items()}
        oracle_colset = {col.normalized_name for col in oracle_columns}
        mapping: list[tuple[str, str]] = []
        for pg_col in pg_columns:
            pg_name = pg_col.normalized_name
            oracle_candidate = pg_to_oracle.get(pg_name, pg_name)
            if oracle_candidate in oracle_colset:
                mapping.append((pg_name, oracle_name(oracle_candidate)))
        return mapping

    def _plan_chunks(
        self,
        ocur,
        owner: str,
        table: str,
        table_cfg: TableConfig,
        where: str | None,
        *,
        chunk_key_override: str | None = None,
        chunk_size_override: int | None = None,
    ) -> list[Chunk]:
        key = [chunk_key_override] if chunk_key_override else (table_cfg.key_columns or table_cfg.primary_key or [])[:1]
        table_fqname = split_schema_table(table_cfg.name, self.config.postgres.schema).fqname
        if not key:
            return [Chunk(table_name=table_fqname, chunk_key="full", where=where)]
        chunk_size = max(1, int(chunk_size_override or self.config.sync.chunk_size or 50000))
        column = key[0]
        try:
            min_value, max_value = oracle.min_max(ocur, owner, table, column)
        except Exception:
            return [Chunk(table_name=table_fqname, chunk_key="full", where=where, primary_key=column)]
        if (
            min_value is None
            or max_value is None
            or not isinstance(min_value, int | float)
            or not isinstance(max_value, int | float)
        ):
            return [Chunk(table_name=table_fqname, chunk_key="full", where=where, primary_key=column)]
        chunks: list[Chunk] = []
        start = int(min_value)
        end_max = int(max_value)
        while start <= end_max:
            end = min(start + chunk_size - 1, end_max)
            chunk_where = _combine_where(where, f"{oracle.qident(column.upper())} BETWEEN {start} AND {end}")
            chunks.append(
                Chunk(
                    table_name=split_schema_table(table_cfg.name, self.config.postgres.schema).fqname,
                    primary_key=column,
                    chunk_key=f"{column}:{start}:{end}",
                    chunk_start=start,
                    chunk_end=end,
                    where=chunk_where,
                )
            )
            start = end + 1
        return chunks

    def _incremental_where(
        self,
        checkpoint_store: CheckpointStore | None,
        table_cfg: TableConfig,
        table_name: str,
        *,
        incremental: bool,
        full_refresh: bool,
    ) -> str | None:
        cfg = table_cfg.incremental
        if full_refresh or not (incremental or cfg.enabled):
            return None
        if cfg.strategy == "oracle_scn":
            raise NotImplementedError(
                "incremental.strategy=oracle_scn belum diimplementasikan; "
                "gunakan updated_at/numeric_key atau --full-refresh"
            )
        if not cfg.column:
            raise ValueError(f"Incremental enabled for {table_name} but incremental.column is empty")
        value = checkpoint_store.get_watermark(
            direction="oracle_to_postgres",
            table_name=table_name,
            strategy=cfg.strategy,
            column_name=cfg.column,
        ) if checkpoint_store else None
        value = value if value not in (None, "") else cfg.initial_value
        if value in (None, ""):
            return None
        column = oracle.qident(cfg.column.upper())
        if cfg.strategy == "numeric_key":
            return f"{column} > {value}"
        if cfg.strategy == "updated_at":
            return (
                f"{column} >= TO_TIMESTAMP('{value}', 'YYYY-MM-DD\"T\"HH24:MI:SS')"
                f" - INTERVAL '{int(cfg.overlap_minutes or 0)}' MINUTE"
            )
        raise ValueError(f"Unsupported incremental strategy: {cfg.strategy}")

    def _build_watermark_candidate(
        self,
        table_cfg: TableConfig,
        table_name: str,
        ocur,
        owner: str,
        table: str,
        where: str | None,
        *,
        enabled: bool,
    ) -> PendingWatermark | None:
        if not enabled or not table_cfg.incremental.enabled:
            return None
        cfg = table_cfg.incremental
        if cfg.strategy == "oracle_scn" or not cfg.column:
            return None
        value = oracle.max_value(ocur, owner, table, cfg.column, where=where)
        if value is not None:
            return PendingWatermark(
                direction="oracle_to_postgres",
                table_name=table_name,
                strategy=cfg.strategy,
                column_name=cfg.column,
                value=value,
            )
        return None

    def _validate_checksum(
        self,
        ocur,
        pcur,
        owner: str,
        target_schema: str,
        source_table: str,
        fq_table: str,
        oracle_columns: list[Any],
        pg_columns_meta: list[Any],
        where: str | None,
        mapping: list[tuple[str, str | None]],
        *,
        target_table: str | None = None,
        force_enabled: bool = False,
    ) -> list[dict[str, Any]]:
        cfg = self.config.validation.checksum
        table_cfg = self.config.table_config(fq_table)
        if table_cfg and table_cfg.validation.checksum.enabled:
            cfg = table_cfg.validation.checksum
        if not cfg.enabled and not force_enabled:
            return []
        pg_cols = checksum_columns(pg_columns_meta, configured=cfg.columns, exclude_columns=cfg.exclude_columns)
        mapped = [(pg_col, oracle_col) for pg_col, oracle_col in mapping if pg_col in pg_cols and oracle_col is not None]
        if not mapped:
            return []
        batch_size = int(getattr(cfg, "batch_size", 5000) or 5000)
        mode = str(getattr(cfg, "mode", "table") or "table").lower()
        chunks = [Chunk(table_name=fq_table, chunk_key="table", where=where)]
        if mode == "chunk":
            chunk_key = getattr(cfg, "chunk_key", None)
            table_cfg_for_chunks = table_cfg or TableConfig(name=fq_table)
            chunks = self._plan_chunks(
                ocur,
                owner,
                source_table,
                table_cfg_for_chunks,
                where,
                chunk_key_override=chunk_key,
            )
        elif mode == "sample":
            self.logger.warning("checksum.mode=sample uses streaming table checksum until DB-specific sampling is configured")
        elif mode != "table":
            raise ValueError(f"Unsupported checksum mode: {mode}")
        rows: list[dict[str, Any]] = []
        pg_table = target_table or split_schema_table(fq_table, self.config.postgres.schema).table
        for chunk in chunks:
            source_cursor = oracle.select_rows(ocur, owner, source_table, mapped, where=_chunk_where(chunk))
            target_cursor = postgres.select_rows(
                pcur,
                target_schema,
                pg_table,
                [pg_col for pg_col, _ in mapped],
                where=_chunk_where(chunk),
            )
            source_hash, source_count = stable_cursor_hash(source_cursor, [pg for pg, _ in mapped], batch_size=batch_size)
            target_hash, target_count = stable_cursor_hash(target_cursor, [pg for pg, _ in mapped], batch_size=batch_size)
            rows.append(
                checksum_result_row(
                    table_name=fq_table,
                    chunk_key=chunk.chunk_key,
                    source_hash=source_hash,
                    target_hash=target_hash,
                    row_count_source=source_count,
                    row_count_target=target_count,
                )
            )
        return rows

    @staticmethod
    def _apply_lob_summary(result: SyncResult, summary: dict[str, Any]) -> None:
        fields = lob_summary_to_fields(summary)
        result.lob_columns_detected = fields["lob_columns_detected"]
        result.lob_columns_synced = fields["lob_columns_synced"]
        result.lob_strategy_applied = fields["lob_strategy_applied"]
        result.lob_columns_skipped = fields["lob_columns_skipped"]
        result.lob_columns_nullified = fields["lob_columns_nullified"]
        result.lob_type = fields["lob_type"]
        result.lob_target_type = fields["lob_target_type"]
        result.lob_validation_mode = fields["lob_validation_mode"]

    def _swap_size_estimate(self, pcur, schema: str, table: str) -> int | None:
        try:
            return postgres.total_relation_size_bytes(pcur, schema, table)
        except Exception:
            self.logger.debug("Cannot estimate PostgreSQL relation size for %s.%s", schema, table, exc_info=True)
            return None

    def _dry_run_message(self, table_name: str, mode: str, column_count: int, swap_size: int | None) -> str:
        message = f"akan load {column_count} kolom ke {table_name}"
        if mode != "swap":
            return message
        estimate = self._estimated_swap_space(swap_size)
        if estimate is None:
            return message + "; mode swap butuh staging table, index staging, WAL/temp, dan lock singkat saat rename"
        return (
            message
            + f"; mode swap estimasi butuh storage tambahan sekitar {self._format_bytes(estimate)}"
            + f" dari table saat ini {self._format_bytes(swap_size)}"
        )

    def _swap_guard_message(self, table_name: str, swap_size: int | None, *, force: bool) -> str:
        if not self.config.sync.allow_swap and not force:
            return (
                "mode swap dinonaktifkan untuk execute karena bisa memenuhi storage/temp RDS; "
                "pakai sync.allow_swap=true atau --force jika sudah review kapasitas"
            )
        max_bytes = self.config.sync.max_swap_table_bytes
        if max_bytes is not None and swap_size is not None and swap_size > int(max_bytes) and not force:
            return (
                f"mode swap di-skip: size {table_name} {self._format_bytes(swap_size)} "
                f"melebihi max_swap_table_bytes {self._format_bytes(int(max_bytes))}"
            )
        return ""

    def _log_swap_risk(self, table_name: str, swap_size: int | None) -> None:
        estimate = self._estimated_swap_space(swap_size)
        if estimate is None:
            self.logger.warning(
                "Mode swap untuk %s membuat staging table, index staging, WAL/temp, dan old table selama transaksi.",
                table_name,
            )
            return
        self.logger.warning(
            "Mode swap untuk %s: current table=%s, perkiraan storage tambahan=%s. "
            "Pastikan free storage RDS cukup.",
            table_name,
            self._format_bytes(swap_size),
            self._format_bytes(estimate),
        )

    def _estimated_swap_space(self, swap_size: int | None) -> int | None:
        if swap_size is None:
            return None
        return int(swap_size * float(self.config.sync.swap_space_multiplier or 2.5))

    @staticmethod
    def _format_bytes(value: int | None) -> str:
        if value is None:
            return "unknown"
        units = ["B", "KiB", "MiB", "GiB", "TiB"]
        size = float(value)
        for unit in units:
            if size < 1024 or unit == units[-1]:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{value} B"

    def _normalize_mode(self, mode: str, *, incremental: bool) -> str:
        value = str(mode or "").lower()
        if value in {
            "truncate",
            "swap",
            "truncate_safe",
            "swap_safe",
            "incremental_safe",
            "append",
        }:
            return value

        if value in {"upsert", "delete"} or incremental:
            return "incremental_safe"

        return value or "truncate_safe"

    def _register_rollback_action(
        self,
        checkpoint_store: CheckpointStore | None,
        result: SyncResult,
        *,
        run_id: str,
        table_name: str,
        action_type: str,
        target_schema: str,
        target_table: str,
        backup_schema: str,
        backup_table: str,
        staging_schema: str,
        staging_table: str,
    ) -> None:
        if not checkpoint_store or not backup_table:
            return
        result.rollback_available = True
        result.rollback_action = action_type
        result.backup_table = f"{backup_schema}.{backup_table}" if backup_schema and backup_table else backup_table
        checkpoint_store.add_rollback_action(
            RollbackAction(
                run_id=run_id,
                table_name=table_name,
                direction="oracle_to_postgres",
                action_type=action_type,
                target_schema=target_schema,
                target_table=target_table,
                backup_schema=backup_schema,
                backup_table=backup_table,
                staging_schema=staging_schema,
                staging_table=staging_table,
            )
        )

    def _finalize_metrics(self, result: SyncResult, metrics: CopyMetrics) -> None:
        self._apply_copy_metrics(result, metrics)
        result.bytes_processed = int(metrics.bytes_processed or 0)
        result.lob_bytes_processed = int(metrics.lob_bytes_processed or 0)
        if result.elapsed_seconds > 0:
            result.rows_per_second = round(float(result.rows_loaded or 0) / result.elapsed_seconds, 3)
            result.bytes_per_second = round(float(result.bytes_processed or 0) / result.elapsed_seconds, 3)

    @staticmethod
    def _apply_copy_metrics(result: SyncResult, metrics: CopyMetrics) -> None:
        result.rows_read_from_oracle = int(getattr(metrics, "rows_read", 0) or 0)
        result.rows_written_to_postgres = int(getattr(metrics, "rows_written", 0) or getattr(metrics, "rows_copied", 0) or 0)
        result.rows_failed = int(getattr(metrics, "rows_failed", 0) or 0)
        result.failed_row_samples = list(getattr(metrics, "failed_row_samples", None) or [])
        if result.lob_columns_synced:
            result.lob_copy_status = "included"
            result.lob_columns_included = result.lob_columns_synced
        elif result.lob_columns_skipped:
            result.lob_copy_status = "skipped"
        elif result.lob_columns_nullified:
            result.lob_copy_status = "nullified"
        else:
            result.lob_copy_status = "none"

    def _validate_copy_completeness(self, result: SyncResult) -> None:
        if result.rows_failed:
            raise RuntimeError(f"copy failed rows={result.rows_failed}; samples={result.failed_row_samples}")
        if result.rows_read_from_oracle != result.rows_written_to_postgres:
            diff = result.rows_written_to_postgres - result.rows_read_from_oracle
            raise RuntimeError(
                "copy row mismatch "
                f"rows_read_from_oracle={result.rows_read_from_oracle} "
                f"rows_written_to_postgres={result.rows_written_to_postgres} diff={diff}"
            )

    def _data_integrity_status(self, result: SyncResult) -> str:
        if result.rows_failed:
            return "FAIL"
        if result.rows_read_from_oracle != result.rows_written_to_postgres:
            return "FAIL"
        if result.checksum_status == "MISMATCH":
            return "FAIL"
        if result.row_count_match is False:
            return "FAIL"
        if result.row_count_match is not True:
            return "UNKNOWN"
        if result.mode in {"truncate", "truncate_safe", "swap", "swap_safe"}:
            if result.postgres_row_count is None:
                return "UNKNOWN"
            if result.rows_written_to_postgres != result.postgres_row_count:
                return "FAIL"
        return "PASS"

    def _data_integrity_failure_message(self, result: SyncResult) -> str:
        if result.rows_failed:
            return f"data integrity failed: rows_failed={result.rows_failed}"
        if result.rows_read_from_oracle != result.rows_written_to_postgres:
            return (
                "data integrity failed: "
                f"rows_read_from_oracle={result.rows_read_from_oracle} "
                f"rows_written_to_postgres={result.rows_written_to_postgres}"
            )
        if result.checksum_status == "MISMATCH":
            return "data integrity failed: checksum mismatch"
        if result.row_count_match is False:
            return (
                "data integrity failed: "
                f"oracle_row_count={result.oracle_row_count} "
                f"postgres_row_count={result.postgres_row_count} "
                f"row_count_diff={result.row_count_diff}"
            )
        if result.postgres_row_count is not None and result.rows_written_to_postgres != result.postgres_row_count:
            return (
                "data integrity failed: "
                f"rows_written_to_postgres={result.rows_written_to_postgres} "
                f"postgres_row_count={result.postgres_row_count}"
            )
        return "data integrity failed"

    def _rowcount_validation_enabled(self, table_cfg: TableConfig) -> bool:
        return bool(table_cfg.validation.rowcount.enabled and self.config.validation.rowcount.enabled)

    def _rowcount_fail_on_mismatch(self, table_cfg: TableConfig) -> bool:
        return bool(table_cfg.validation.rowcount.fail_on_mismatch and self.config.validation.rowcount.fail_on_mismatch)

    def _precheck_skip_if_rowcount_match(
        self,
        result: SyncResult,
        table_cfg: TableConfig,
        mode: str,
        incremental_where: str | None,
        *,
        full_refresh: bool,
        ocur,
        pcur,
        owner: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        where: str | None,
    ) -> SyncResult | None:
        if not self._should_skip_if_rowcount_match(table_cfg, mode, where, incremental_where, full_refresh=full_refresh):
            return None
        result.oracle_row_count, result.postgres_row_count, result.row_count_match = self._safe_rowcount_validation(
            ocur,
            pcur,
            owner,
            source_table,
            target_schema,
            target_table,
            where,
        )
        result.row_count_diff = (result.postgres_row_count or 0) - (result.oracle_row_count or 0)
        result.validation_status = "validation_pass" if result.row_count_match else "validation_failed"
        if result.row_count_match is not True:
            return None
        result.status = "SKIPPED"
        result.data_integrity_status = "PASS"
        result.message = "skip sync: source/target rowcount already match before load"
        self.logger.info(
            "Skip %s karena rowcount source/target sudah match sebelum load source=%s target=%s",
            result.table_name,
            result.oracle_row_count,
            result.postgres_row_count,
        )
        return result

    def _should_skip_if_rowcount_match(
        self,
        table_cfg: TableConfig,
        mode: str,
        where: str | None,
        incremental_where: str | None,
        *,
        full_refresh: bool,
    ) -> bool:
        if not self.config.sync.skip_if_rowcount_match:
            return False
        if mode not in {"truncate", "truncate_safe", "swap", "swap_safe"}:
            return False
        if where or incremental_where:
            return False
        if table_cfg.where:
            return False
        if table_cfg.incremental.enabled and not full_refresh:
            return False
        return True

    def _safe_rowcount_validation(
        self,
        ocur,
        pcur,
        owner: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        where: str | None,
    ) -> tuple[int, int, bool]:
        source_count = oracle.count_rows_where(ocur, owner, source_table, where)
        target_count = postgres.count_rows_where(pcur, target_schema, target_table)
        return source_count, target_count, source_count == target_count

    def _cleanup_backup_retention(self, pcur, schema: str, table: str) -> None:
        keep = max(0, int(self.config.sync.backup_retention_count or 0))
        if keep <= 0:
            return
        candidates = postgres.list_matching_tables(pcur, schema, f"{table}__backup_%")
        if len(candidates) <= keep:
            return
        postgres.drop_tables(pcur, schema, candidates[keep:])

    def _cleanup_staging_retention(self, pcur, schema: str, table: str) -> None:
        keep = max(0, int(self.config.sync.staging_retention_count or 0))
        if keep <= 0:
            return
        candidates = postgres.list_matching_tables(pcur, schema, f"_stg_{table}_%")
        if len(candidates) <= keep:
            return
        postgres.drop_tables(pcur, schema, candidates[keep:])


def table_ident(schema: str, table: str):
    return postgres.table_ident(schema, table)


def _combine_where(left: str | None, right: str | None) -> str | None:
    if left and right:
        return f"({left}) AND ({right})"
    return left or right


def _chunk_where(chunk: Chunk) -> str | None:
    return chunk.where


def _merge_copy_metrics(target: CopyMetrics | None, source: CopyMetrics) -> None:
    if target is None:
        return
    target.rows_read += int(source.rows_read or 0)
    target.rows_written += int(source.rows_written or 0)
    target.rows_failed += int(source.rows_failed or 0)
    target.rows_copied += int(source.rows_copied or 0)
    target.bytes_processed += int(source.bytes_processed or 0)
    target.lob_bytes_processed += int(source.lob_bytes_processed or 0)
    if source.failed_row_samples:
        if target.failed_row_samples is None:
            target.failed_row_samples = []
        target.failed_row_samples.extend(source.failed_row_samples)


def _oracle_count_sql_summary(owner: str, table: str, where: str | None) -> str:
    summary = f"SELECT COUNT(1) FROM {owner}.{table}"
    if where:
        summary += f" WHERE {where}"
    return summary


def _postgres_count_sql_summary(schema: str, table: str) -> str:
    return f"SELECT COUNT(1) FROM {schema}.{table}"


def _apply_checksum_summary(result: SyncResult, rows: list[dict[str, Any]]) -> None:
    result.checksum_status = "MISMATCH" if any(row.get("status") == "MISMATCH" for row in rows) else "MATCH"
    result.checksum_source_rows = sum(int(row.get("row_count_source") or 0) for row in rows)
    result.checksum_target_rows = sum(int(row.get("row_count_target") or 0) for row in rows)
    if len(rows) == 1:
        result.checksum_source_hash = str(rows[0].get("source_hash") or "")
        result.checksum_target_hash = str(rows[0].get("target_hash") or "")
    else:
        result.checksum_source_hash = f"{len(rows)} chunks"
        result.checksum_target_hash = f"{len(rows)} chunks"
