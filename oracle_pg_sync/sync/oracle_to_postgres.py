from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from oracle_pg_sync.config import AppConfig, TableConfig
from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.metadata.compare import compare_table_metadata, inventory_has_fatal_mismatch
from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata
from oracle_pg_sync.sync.copy_loader import copy_rows
from oracle_pg_sync.sync.staging import atomic_swap, create_staging_like, drop_table
from oracle_pg_sync.utils.naming import oracle_name, split_schema_table


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

    def as_row(self) -> dict[str, Any]:
        return {
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
    ) -> list[SyncResult]:
        workers = max(1, int(self.config.sync.parallel_workers or 1))
        if workers == 1:
            return [
                self.sync_table(table, mode_override=mode_override, execute=execute, force=force)
                for table in tables
            ]

        results: list[SyncResult] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self.sync_table, table, mode_override=mode_override, execute=execute, force=force): table
                for table in tables
            }
            for future in as_completed(futures):
                results.append(future.result())
        return sorted(results, key=lambda r: r.table_name)

    def sync_table(
        self,
        table_name: str,
        *,
        mode_override: str | None = None,
        execute: bool = False,
        force: bool = False,
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

        result = SyncResult(table.fqname, mode, "PENDING", dry_run=dry_run)
        self.logger.info("Sync %s mode=%s dry_run=%s", table.fqname, mode, dry_run)

        try:
            with oracle.connect(self.config.oracle) as ocon, postgres.connect(self.config.postgres) as pcon:
                with ocon.cursor() as ocur, pcon.cursor() as pcur:
                    owner = self.config.oracle.schema
                    oracle_meta = fetch_oracle_metadata(
                        ocur,
                        owner=owner,
                        table=table.table,
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

                    pg_columns = [pg_col for pg_col, _ in mapping]
                    if dry_run:
                        result.status = "DRY_RUN"
                        result.message = f"akan load {len(pg_columns)} kolom ke {table.fqname}"
                        return result

                    if mode == "truncate":
                        self.logger.warning(
                            "Mode truncate menjaga index/view dependency, tapi PostgreSQL mengambil ACCESS EXCLUSIVE lock. "
                            "lock_timeout=%s",
                            self.config.sync.pg_lock_timeout,
                        )
                        rows = self._sync_truncate(ocur, pcur, owner, table.schema, table.table, mapping, table_cfg.where)
                    elif mode == "swap":
                        rows = self._sync_swap(ocur, pcur, owner, table.schema, table.table, mapping, table_cfg.where)
                    elif mode == "append":
                        rows = self._copy_oracle_to_pg(ocur, pcur, owner, table.schema, table.table, table.table, mapping, table_cfg.where)
                    elif mode == "upsert":
                        rows = self._sync_upsert(
                            ocur,
                            pcur,
                            owner,
                            table.schema,
                            table.table,
                            mapping,
                            table_cfg.key_columns,
                            table_cfg.where,
                        )
                    else:
                        raise ValueError(f"Unsupported sync mode: {mode}")

                    result.rows_loaded = rows
                    if self.config.sync.analyze_after_load:
                        postgres.analyze_table(pcur, table.schema, table.table)

                    if self.config.sync.exact_count_after_load:
                        result.oracle_row_count = oracle.count_rows(ocur, owner, table.table)
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
        mapping: list[tuple[str, str]],
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
        mapping: list[tuple[str, str]],
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
        table: str,
        mapping: list[tuple[str, str]],
        key_columns: list[str],
        where: str | None,
    ) -> int:
        if not key_columns:
            raise ValueError(f"Mode upsert but key_columns is empty for {schema}.{table}")
        staging = create_staging_like(pcur, schema, table)
        rows = self._copy_oracle_to_pg(ocur, pcur, owner, schema, table, staging, mapping, where)
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
            table_ident(schema, table),
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
        mapping: list[tuple[str, str]],
        where: str | None,
    ) -> int:
        rows_cursor = oracle.select_rows(ocur, owner, source_table, mapping, where=where)
        pg_columns = [pg_col for pg_col, _ in mapping]
        return copy_rows(pcur, schema=source_schema, table=target_table, columns=pg_columns, rows=rows_cursor)

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


def table_ident(schema: str, table: str):
    return postgres.table_ident(schema, table)
