from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from oracle_pg_sync.config import AppConfig, TableConfig
from oracle_pg_sync.db import oracle, postgres
from oracle_pg_sync.metadata.compare import compare_table_metadata, inventory_has_fatal_mismatch
from oracle_pg_sync.metadata.oracle_metadata import fetch_table_metadata as fetch_oracle_metadata
from oracle_pg_sync.metadata.postgres_metadata import fetch_table_metadata as fetch_pg_metadata
from oracle_pg_sync.utils.naming import split_schema_table


@dataclass
class ReverseSyncResult:
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
            "direction": "postgres-to-oracle",
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


class PostgresToOracleSync:
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
    ) -> list[ReverseSyncResult]:
        workers = max(1, int(self.config.sync.parallel_workers or 1))
        if workers == 1:
            return [
                self.sync_table(table, mode_override=mode_override, execute=execute, force=force)
                for table in tables
            ]

        results: list[ReverseSyncResult] = []
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
    ) -> ReverseSyncResult:
        started = time.time()
        table = split_schema_table(table_name, self.config.postgres.schema)
        table_cfg = self.config.table_config(table_name) or TableConfig(name=table_name)
        mode = (
            mode_override
            or table_cfg.postgres_to_oracle_mode
            or table_cfg.mode
            or "truncate"
        ).lower()
        dry_run = not execute or self.config.sync.dry_run and not execute
        result = ReverseSyncResult(table.fqname, mode, "PENDING", dry_run=dry_run)

        self.logger.info("Reverse sync %s mode=%s dry_run=%s", table.fqname, mode, dry_run)

        try:
            if mode == "swap":
                result.status = "SKIPPED"
                result.message = "swap PostgreSQL -> Oracle tidak diaktifkan karena berisiko untuk grants/views/triggers"
                return result

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
                        result.message = "struktur/table mismatch; gunakan --force jika tetap ingin reverse sync"
                        return result

                    mapping = self._column_mapping(table.fqname, pg_meta.columns, oracle_meta.columns)
                    if not mapping:
                        result.status = "SKIPPED"
                        result.message = "tidak ada kolom yang bisa dimapping"
                        return result

                    if dry_run:
                        result.status = "DRY_RUN"
                        result.message = f"akan load {len(mapping)} kolom dari PostgreSQL ke Oracle"
                        return result

                    if mode == "truncate":
                        rows = self._sync_truncate(pcur, ocur, table.schema, table.table, owner, mapping, table_cfg.where)
                    elif mode == "delete":
                        rows = self._sync_delete(pcur, ocur, table.schema, table.table, owner, mapping, table_cfg.where)
                    elif mode == "append":
                        rows = self._copy_pg_to_oracle(pcur, ocur, table.schema, table.table, owner, mapping, table_cfg.where)
                    elif mode == "upsert":
                        rows = self._sync_upsert(
                            pcur,
                            ocur,
                            table.schema,
                            table.table,
                            owner,
                            mapping,
                            table_cfg.key_columns,
                            table_cfg.where,
                        )
                    else:
                        raise ValueError(f"Unsupported reverse sync mode: {mode}")

                    result.rows_loaded = rows
                    if self.config.sync.exact_count_after_load:
                        result.oracle_row_count = oracle.count_rows(ocur, owner, table.table)
                        result.postgres_row_count = postgres.count_rows(pcur, table.schema, table.table)
                        result.row_count_match = result.oracle_row_count == result.postgres_row_count
                        if result.row_count_match:
                            result.status = "SUCCESS"
                        else:
                            result.status = "WARNING"
                            result.message = "rowcount berbeda setelah reverse sync"
                    else:
                        result.status = "SUCCESS"

                    ocon.commit()
                    return result
        except Exception as exc:
            result.status = "FAILED"
            result.message = str(exc)
            self.logger.exception("Reverse sync failed for %s", table.fqname)
            return result
        finally:
            result.elapsed_seconds = time.time() - started

    def _sync_truncate(
        self,
        pcur,
        ocur,
        pg_schema: str,
        table: str,
        oracle_owner: str,
        mapping: list[tuple[str, str]],
        where: str | None,
    ) -> int:
        oracle.truncate_table(ocur, oracle_owner, table)
        return self._copy_pg_to_oracle(pcur, ocur, pg_schema, table, oracle_owner, mapping, where)

    def _sync_delete(
        self,
        pcur,
        ocur,
        pg_schema: str,
        table: str,
        oracle_owner: str,
        mapping: list[tuple[str, str]],
        where: str | None,
    ) -> int:
        oracle.delete_rows(ocur, oracle_owner, table)
        return self._copy_pg_to_oracle(pcur, ocur, pg_schema, table, oracle_owner, mapping, where)

    def _sync_upsert(
        self,
        pcur,
        ocur,
        pg_schema: str,
        table: str,
        oracle_owner: str,
        mapping: list[tuple[str, str]],
        key_columns: list[str],
        where: str | None,
    ) -> int:
        if not key_columns:
            raise ValueError(f"Mode upsert but key_columns is empty for {pg_schema}.{table}")
        return self._copy_pg_to_oracle(
            pcur,
            ocur,
            pg_schema,
            table,
            oracle_owner,
            mapping,
            where,
            upsert_keys=[col.lower() for col in key_columns],
        )

    def _copy_pg_to_oracle(
        self,
        pcur,
        ocur,
        pg_schema: str,
        table: str,
        oracle_owner: str,
        mapping: list[tuple[str, str]],
        where: str | None,
        upsert_keys: list[str] | None = None,
    ) -> int:
        oracle_columns = [oracle_col for oracle_col, _ in mapping]
        pg_columns = [pg_col for _, pg_col in mapping]
        rows_cursor = postgres.select_rows(pcur, pg_schema, table, pg_columns, where=where)
        total = 0
        batch_size = max(1, int(self.config.sync.batch_size or 10000))
        while True:
            rows = rows_cursor.fetchmany(batch_size)
            if not rows:
                break
            clean_rows = [tuple(_clean_value(value) for value in row) for row in rows]
            if upsert_keys:
                total += oracle.merge_rows(
                    ocur,
                    owner=oracle_owner,
                    table=table,
                    oracle_columns=oracle_columns,
                    key_columns=upsert_keys,
                    rows=clean_rows,
                )
            else:
                total += oracle.insert_rows(
                    ocur,
                    owner=oracle_owner,
                    table=table,
                    oracle_columns=oracle_columns,
                    rows=clean_rows,
                )
        return total

    def _column_mapping(
        self,
        fq_table: str,
        pg_columns: list[Any],
        oracle_columns: list[Any],
    ) -> list[tuple[str, str]]:
        rename = self.config.rename_columns.get(fq_table.lower(), {})
        pg_colset = {col.normalized_name for col in pg_columns}
        mapping: list[tuple[str, str]] = []
        for oracle_col in oracle_columns:
            oracle_name_l = oracle_col.normalized_name
            pg_candidate = rename.get(oracle_name_l, oracle_name_l)
            if pg_candidate in pg_colset:
                mapping.append((oracle_name_l, pg_candidate))
        return mapping


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value
