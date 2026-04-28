from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from oracle_pg_sync.checkpoint import CheckpointStore, Chunk, new_run_id
from oracle_pg_sync.config import AppConfig, TableConfig
from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.lob import apply_lob_mapping_policy
from oracle_pg_sync.metadata.compare import compare_table_metadata, inventory_has_fatal_mismatch
from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata
from oracle_pg_sync.sync.copy_loader import copy_rows
from oracle_pg_sync.sync.staging import atomic_swap, create_staging_like, drop_table
from oracle_pg_sync.utils.naming import oracle_name, split_schema_table
from oracle_pg_sync.validation import checksum_columns, checksum_result_row, stable_row_hash


@dataclass
class SyncResult:
    table_name: str
    mode: str
    status: str
    rows_loaded: int = 0
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
    lob_columns_detected: str = ""
    lob_strategy_applied: str = ""
    lob_columns_skipped: str = ""
    lob_columns_nullified: str = ""

    def as_row(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "table_name": self.table_name,
            "direction": "oracle-to-postgres",
            "mode": self.mode,
            "status": self.status,
            "rows_loaded": self.rows_loaded,
            "oracle_row_count": self.oracle_row_count,
            "postgres_row_count": self.postgres_row_count,
            "row_count_match": self.row_count_match,
            "dry_run": self.dry_run,
            "message": self.message,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "checksum_status": self.checksum_status,
            "checksum_source_hash": self.checksum_source_hash,
            "checksum_target_hash": self.checksum_target_hash,
            "checksum_source_rows": self.checksum_source_rows,
            "checksum_target_rows": self.checksum_target_rows,
            "lob_columns_detected": self.lob_columns_detected,
            "lob_strategy_applied": self.lob_strategy_applied,
            "lob_columns_skipped": self.lob_columns_skipped,
            "lob_columns_nullified": self.lob_columns_nullified,
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
            )
        workers = max(1, int(self.config.sync.parallel_workers or 1))
        if workers == 1:
            results = []
            for table in tables:
                results.append(
                    self.sync_table(
                        table,
                        mode_override=mode_override,
                        execute=execute,
                        force=force,
                        checkpoint_store=checkpoint_store,
                        run_id=run_id,
                        resume=resume,
                        incremental=incremental,
                        full_refresh=full_refresh,
                    )
                )
            if checkpoint_store:
                checkpoint_store.finish_run(
                    run_id,
                    status="failed" if any(result.status == "FAILED" for result in results) else "success",
                )
            return results

        results: list[SyncResult] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    self.sync_table,
                    table,
                    mode_override=mode_override,
                    execute=execute,
                    force=force,
                    checkpoint_store=checkpoint_store,
                    run_id=run_id,
                    resume=resume,
                    incremental=incremental,
                    full_refresh=full_refresh,
                ): table
                for table in tables
            }
            for future in as_completed(futures):
                results.append(future.result())
        results = sorted(results, key=lambda r: r.table_name)
        if checkpoint_store:
            checkpoint_store.finish_run(
                run_id,
                status="failed" if any(result.status == "FAILED" for result in results) else "success",
            )
        return results

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
    ) -> SyncResult:
        started = time.time()
        table = split_schema_table(table_name, self.config.postgres.schema)
        table_cfg = self.config.table_config(table_name) or TableConfig(name=table_name)
        mode = (
            mode_override
            or table_cfg.oracle_to_postgres_mode
            or table_cfg.mode
            or self.config.sync.default_mode
        ).lower()
        dry_run = not execute or self.config.sync.dry_run and not execute

        run_id = run_id or new_run_id()
        result = SyncResult(table.fqname, mode, "PENDING", dry_run=dry_run, run_id=run_id)
        self.logger.info("Sync %s mode=%s dry_run=%s", table.fqname, mode, dry_run)

        try:
            with oracle.connect(self.config.oracle) as ocon, postgres.connect(self.config.postgres) as pcon:
                with ocon.cursor() as ocur, pcon.cursor() as pcur:
                    owner = table_cfg.source_schema or self.config.oracle.schema
                    source_table = table_cfg.source_table or table.table
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
                    swap_size = self._swap_size_estimate(pcur, table.schema, table.table) if mode == "swap" else None
                    base_where = table_cfg.where
                    incremental_where = self._incremental_where(
                        checkpoint_store,
                        table_cfg,
                        table.fqname,
                        incremental=incremental,
                        full_refresh=full_refresh,
                    )
                    effective_where = _combine_where(base_where, incremental_where)
                    if dry_run:
                        result.status = "DRY_RUN"
                        result.message = self._dry_run_message(table.fqname, mode, len(pg_columns), swap_size)
                        if incremental_where:
                            result.message += f"; incremental filter: {incremental_where}"
                        return result

                    chunks = self._plan_chunks(ocur, owner, source_table, table_cfg, effective_where)
                    successful = checkpoint_store.successful_chunks(run_id, table.fqname) if checkpoint_store and resume else set()
                    if mode == "truncate" and not successful:
                        postgres.set_local_timeouts(
                            pcur,
                            lock_timeout=self.config.sync.pg_lock_timeout,
                            statement_timeout=self.config.sync.pg_statement_timeout,
                        )
                        postgres.truncate_table(pcur, table.schema, table.table, cascade=self.config.sync.truncate_cascade)

                    if mode == "truncate":
                        self.logger.warning(
                            "Mode truncate menjaga index/view dependency, tapi PostgreSQL mengambil ACCESS EXCLUSIVE lock. "
                            "lock_timeout=%s",
                            self.config.sync.pg_lock_timeout,
                        )
                        rows = self._copy_chunks(
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
                        )
                    elif mode == "swap":
                        guard_message = self._swap_guard_message(table.fqname, swap_size, force=force)
                        if guard_message:
                            result.status = "SKIPPED"
                            result.message = guard_message
                            return result
                        self._log_swap_risk(table.fqname, swap_size)
                        rows = self._sync_swap(ocur, pcur, owner, table.schema, table.table, mapping, effective_where)
                    elif mode == "append":
                        rows = self._copy_chunks(
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
                        )
                    elif mode == "upsert":
                        rows = self._sync_upsert(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            source_table,
                            table.table,
                            mapping,
                            table_cfg.key_columns,
                            effective_where,
                        )
                    else:
                        raise ValueError(f"Unsupported sync mode: {mode}")

                    result.rows_loaded = rows
                    if self.config.sync.analyze_after_load:
                        postgres.analyze_table(pcur, table.schema, table.table)
                    checksum_row = self._validate_checksum(
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
                    if checksum_row:
                        result.checksum_status = checksum_row["status"]
                        result.checksum_source_hash = checksum_row["source_hash"]
                        result.checksum_target_hash = checksum_row["target_hash"]
                        result.checksum_source_rows = checksum_row["row_count_source"]
                        result.checksum_target_rows = checksum_row["row_count_target"]
                        if checksum_row["status"] == "MISMATCH":
                            result.status = "FAILED"
                            result.message = "checksum mismatch after load"
                            pcon.rollback()
                            return result

                    if self.config.sync.exact_count_after_load:
                        result.oracle_row_count = oracle.count_rows(ocur, owner, source_table)
                        result.postgres_row_count = postgres.count_rows(pcur, table.schema, table.table)
                        result.row_count_match = result.oracle_row_count == result.postgres_row_count
                        if not result.row_count_match:
                            result.status = "WARNING"
                            result.message = "rowcount berbeda setelah load"
                        else:
                            result.status = "SUCCESS"
                    else:
                        result.status = "SUCCESS"

                    pcon.commit()
                    self._update_watermark(
                        checkpoint_store,
                        table_cfg,
                        table.fqname,
                        ocur,
                        owner,
                        source_table,
                        effective_where,
                        enabled=incremental or table_cfg.incremental.enabled,
                    )
                    return result
        except Exception as exc:
            result.status = "FAILED"
            result.message = str(exc)
            self.logger.exception("Sync failed for %s", table.fqname)
            return result
        finally:
            result.elapsed_seconds = time.time() - started

    def _sync_truncate(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        table: str,
        mapping: list[tuple[str, str | None]],
        where: str | None,
    ) -> int:
        postgres.set_local_timeouts(
            pcur,
            lock_timeout=self.config.sync.pg_lock_timeout,
            statement_timeout=self.config.sync.pg_statement_timeout,
        )
        postgres.truncate_table(pcur, schema, table, cascade=self.config.sync.truncate_cascade)
        return self._copy_oracle_to_pg(ocur, pcur, owner, schema, table, table, mapping, where)

    def _sync_swap(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        table: str,
        mapping: list[tuple[str, str | None]],
        where: str | None,
    ) -> int:
        postgres.set_local_timeouts(
            pcur,
            lock_timeout=self.config.sync.pg_lock_timeout,
            statement_timeout=self.config.sync.pg_statement_timeout,
        )
        staging = create_staging_like(pcur, schema, table)
        rows = self._copy_oracle_to_pg(ocur, pcur, owner, schema, table, staging, mapping, where)
        if self.config.sync.exact_count_after_load:
            source_count = oracle.count_rows(ocur, owner, table)
            staging_count = postgres.count_rows(pcur, schema, staging)
            if source_count != staging_count:
                raise RuntimeError(f"staging rowcount mismatch Oracle={source_count} PG={staging_count}")
        if self.config.sync.analyze_after_load:
            postgres.analyze_table(pcur, schema, staging)
        old_table = atomic_swap(pcur, schema, table)
        if not self.config.sync.keep_old_after_swap:
            drop_table(pcur, schema, old_table)
        return rows

    def _sync_upsert(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        source_table: str,
        target_table: str,
        mapping: list[tuple[str, str | None]],
        key_columns: list[str],
        where: str | None,
    ) -> int:
        if not key_columns:
            raise ValueError(f"Mode upsert but key_columns is empty for {schema}.{target_table}")
        staging = create_staging_like(pcur, schema, target_table)
        rows = self._copy_oracle_to_pg(ocur, pcur, owner, schema, source_table, staging, mapping, where)
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
            table_ident(schema, staging),
            conflict_action,
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in pg_columns),
            keys=sql.SQL(", ").join(sql.Identifier(c.lower()) for c in key_columns),
        )
        pcur.execute(insert_stmt)
        drop_table(pcur, schema, staging)
        return rows

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
    ) -> int:
        rows_cursor = oracle.select_rows(ocur, owner, source_table, mapping, where=where)
        pg_columns = [pg_col for pg_col, _ in mapping]
        return copy_rows(pcur, schema=source_schema, table=target_table, columns=pg_columns, rows=rows_cursor)

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
    ) -> int:
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
            if checkpoint_store:
                checkpoint_store.start_chunk(run_id, chunk.table_name, chunk.chunk_key)
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
    ) -> list[Chunk]:
        key = (table_cfg.key_columns or table_cfg.primary_key or [])[:1]
        if not key:
            return [Chunk(table_name=split_schema_table(table_cfg.name, self.config.postgres.schema).fqname, chunk_key="full", where=where)]
        chunk_size = max(1, int(self.config.sync.chunk_size or 50000))
        column = key[0]
        try:
            min_value, max_value = oracle.min_max(ocur, owner, table, column)
        except Exception:
            return [Chunk(table_name=split_schema_table(table_cfg.name, self.config.postgres.schema).fqname, chunk_key="full", where=where, primary_key=column)]
        if min_value is None or max_value is None or not isinstance(min_value, int | float) or not isinstance(max_value, int | float):
            return [Chunk(table_name=split_schema_table(table_cfg.name, self.config.postgres.schema).fqname, chunk_key="full", where=where, primary_key=column)]
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
            raise NotImplementedError("incremental.strategy=oracle_scn belum diimplementasikan; gunakan updated_at/numeric_key atau --full-refresh")
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

    def _update_watermark(
        self,
        checkpoint_store: CheckpointStore | None,
        table_cfg: TableConfig,
        table_name: str,
        ocur,
        owner: str,
        table: str,
        where: str | None,
        *,
        enabled: bool,
    ) -> None:
        if not checkpoint_store or not enabled or not table_cfg.incremental.enabled:
            return
        cfg = table_cfg.incremental
        if cfg.strategy == "oracle_scn" or not cfg.column:
            return
        value = oracle.max_value(ocur, owner, table, cfg.column, where=where)
        if value is not None:
            checkpoint_store.set_watermark(
                direction="oracle_to_postgres",
                table_name=table_name,
                strategy=cfg.strategy,
                column_name=cfg.column,
                value=value,
            )

    def _validate_checksum(
        self,
        ocur,
        pcur,
        owner: str,
        schema: str,
        table: str,
        fq_table: str,
        oracle_columns: list[Any],
        pg_columns_meta: list[Any],
        where: str | None,
        mapping: list[tuple[str, str | None]],
    ) -> dict[str, Any] | None:
        cfg = self.config.validation.checksum
        table_cfg = self.config.table_config(fq_table)
        if table_cfg and table_cfg.validation.checksum.enabled:
            cfg = table_cfg.validation.checksum
        if not cfg.enabled:
            return None
        pg_cols = checksum_columns(pg_columns_meta, configured=cfg.columns, exclude_columns=cfg.exclude_columns)
        mapped = [(pg_col, oracle_col) for pg_col, oracle_col in mapping if pg_col in pg_cols and oracle_col is not None]
        if not mapped:
            return None
        source_rows = list(oracle.select_rows(ocur, owner, table, mapped, where=where).fetchall())
        target_rows = list(postgres.select_rows(pcur, schema, table, [pg_col for pg_col, _ in mapped], where=where).fetchall())
        return checksum_result_row(
            table_name=fq_table,
            chunk_key="table",
            source_hash=stable_row_hash(source_rows, [pg for pg, _ in mapped]),
            target_hash=stable_row_hash(target_rows, [pg for pg, _ in mapped]),
            row_count_source=len(source_rows),
            row_count_target=len(target_rows),
        )

    @staticmethod
    def _apply_lob_summary(result: SyncResult, summary: dict[str, Any]) -> None:
        result.lob_columns_detected = ";".join(summary.get("lob_columns_detected") or [])
        result.lob_strategy_applied = ";".join(
            f"{k}:{v}" for k, v in (summary.get("lob_strategy_applied") or {}).items()
        )
        result.lob_columns_skipped = ";".join(summary.get("lob_columns_skipped") or [])
        result.lob_columns_nullified = ";".join(summary.get("lob_columns_nullified") or [])

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


def table_ident(schema: str, table: str):
    return postgres.table_ident(schema, table)


def _combine_where(left: str | None, right: str | None) -> str | None:
    if left and right:
        return f"({left}) AND ({right})"
    return left or right


def _chunk_where(chunk: Chunk) -> str | None:
    return chunk.where
