from __future__ import annotations

import logging
from typing import Any

from oracle_pg_sync.checkpoint import CheckpointStore
from oracle_pg_sync.config import AppConfig
from oracle_pg_sync.sync.staging import restore_backup_table


def rollback_run(
    config: AppConfig,
    checkpoint_store: CheckpointStore,
    *,
    run_id: str,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    from oracle_pg_sync.db import oracle, postgres

    logger = logger or logging.getLogger("oracle_pg_sync")
    actions = checkpoint_store.rollback_actions(run_id)
    if not actions:
        return [{"run_id": run_id, "status": "FAILED", "message": "no rollback actions found"}]
    rows: list[dict[str, Any]] = []
    pg_actions = [action for action in actions if str(action.get("direction")) == "oracle_to_postgres"]
    if pg_actions:
        with postgres.connect(config.postgres) as pcon:
            with pcon.cursor() as pcur:
                for action in pg_actions:
                    rows.append(_rollback_postgres_action(pcur, checkpoint_store, action, logger))
            pcon.commit()
    if any(row.get("status") == "FAILED" for row in rows):
        return rows
    try:
        with oracle.connect(config.oracle) as ocon, postgres.connect(config.postgres, autocommit=True) as pcon:
            with ocon.cursor() as ocur, pcon.cursor() as pcur:
                if config.dependency.auto_recompile_oracle:
                    oracle.compile_invalid_objects(ocur, config.oracle.schema)
                    ocon.commit()
                if config.dependency.refresh_postgres_mview:
                    postgres.refresh_materialized_views(pcur, pg_actions)
    except Exception:
        logger.exception("Dependency restore after rollback failed")
    return rows


def _rollback_postgres_action(pcur, checkpoint_store: CheckpointStore, action: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    from oracle_pg_sync.db import postgres

    table_name = str(action.get("table_name") or "")
    action_type = str(action.get("action_type") or "")
    schema = str(action.get("target_schema") or "")
    table = str(action.get("target_table") or "")
    backup_schema = str(action.get("backup_schema") or schema)
    backup_table = str(action.get("backup_table") or "")
    result = {
        "run_id": action.get("run_id"),
        "table_name": table_name,
        "action_type": action_type,
        "backup_table": backup_table,
        "status": "SUCCESS",
        "message": "",
    }
    try:
        if not backup_table:
            raise RuntimeError("backup table missing")
        if action_type == "swap_safe":
            restore_backup_table(pcur, schema, table, backup_table)
        elif action_type in {"truncate_safe", "incremental_safe"}:
            postgres.truncate_table(pcur, schema, table)
            columns = [row["name"] for row in postgres.get_columns(pcur, schema, table)]
            postgres.insert_from_table(
                pcur,
                target_schema=schema,
                target_table=table,
                source_schema=backup_schema,
                source_table=backup_table,
                columns=columns,
            )
        else:
            raise RuntimeError(f"unsupported rollback action: {action_type}")
        checkpoint_store.mark_rollback_action(
            str(action.get("run_id") or ""),
            table_name,
            action_type,
            status="restored",
            notes="rollback completed",
        )
    except Exception as exc:
        logger.exception("Rollback failed for %s", table_name)
        checkpoint_store.mark_rollback_action(
            str(action.get("run_id") or ""),
            table_name,
            action_type,
            status="failed",
            notes=str(exc),
        )
        result["status"] = "FAILED"
        result["message"] = str(exc)
    return result
