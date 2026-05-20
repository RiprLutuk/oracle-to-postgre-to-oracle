from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from psycopg import sql

from oracle_pg_sync.db.postgres import table_ident


@dataclass
class CopyMetrics:
    rows_read: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    rows_copied: int = 0
    bytes_processed: int = 0
    lob_bytes_processed: int = 0
    failed_row_samples: list[dict[str, Any]] | None = None

    def add_failed_sample(self, sample: dict[str, Any], *, limit: int) -> None:
        if self.failed_row_samples is None:
            self.failed_row_samples = []
        if len(self.failed_row_samples) < max(0, int(limit)):
            self.failed_row_samples.append(sample)


class CopyRowError(RuntimeError):
    pass


def copy_rows(
    pg_cur,
    *,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[tuple[Any, ...]],
    lob_chunk_size_bytes: int = 1024 * 1024,
    metrics: CopyMetrics | None = None,
    table_name: str = "",
    chunk_key: str = "",
    key_columns: list[str] | None = None,
    skip_failed_rows: bool = False,
    failed_row_sample_limit: int = 20,
    trim_columns: set[str] | None = None,
) -> int:
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        table_ident(schema, table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )
    copied = 0
    active_metrics = metrics or CopyMetrics()
    with pg_cur.copy(copy_sql) as copy:
        for row_number, row in enumerate(rows, start=1):
            active_metrics.rows_read += 1
            try:
                sanitized = _sanitize_row(
                    row,
                    columns=columns,
                    lob_chunk_size_bytes=lob_chunk_size_bytes,
                    metrics=active_metrics,
                    trim_columns={column.lower() for column in (trim_columns or set())},
                )
                copy.write_row(sanitized)
                copied += 1
                active_metrics.rows_written += 1
                active_metrics.rows_copied += 1
            except Exception as exc:
                active_metrics.rows_failed += 1
                sample = _failed_row_sample(
                    table_name=table_name or f"{schema}.{table}",
                    chunk_key=chunk_key,
                    row_number=row_number,
                    key_columns=key_columns or [],
                    columns=columns,
                    row=row,
                    error=exc,
                    column_name=getattr(exc, "column_name", ""),
                )
                active_metrics.add_failed_sample(sample, limit=failed_row_sample_limit)
                if skip_failed_rows:
                    continue
                raise CopyRowError(_format_failed_row_message(sample)) from exc
    return copied


def _sanitize_row(
    row: tuple[Any, ...],
    *,
    columns: list[str],
    lob_chunk_size_bytes: int,
    metrics: CopyMetrics,
    trim_columns: set[str] | None = None,
) -> list[Any]:
    sanitized = []
    trim_columns = trim_columns or set()
    for idx, value in enumerate(row):
        column_name = columns[idx] if idx < len(columns) else f"column_{idx + 1}"
        try:
            if isinstance(value, str) and column_name.lower() in trim_columns:
                value = value.strip()
            sanitized.append(_sanitize_value(value, lob_chunk_size_bytes=lob_chunk_size_bytes, metrics=metrics))
        except Exception as exc:
            setattr(exc, "column_name", column_name)
            raise
    return sanitized


def _sanitize_value(
    value: Any,
    *,
    lob_chunk_size_bytes: int = 1024 * 1024,
    metrics: CopyMetrics | None = None,
) -> Any:
    if value is None:
        return None
    is_lob = hasattr(value, "read")
    if hasattr(value, "read"):
        value = _read_lob_stream(value, chunk_size=max(1, int(lob_chunk_size_bytes or 1024 * 1024)))
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if metrics:
        size = _value_size(value)
        metrics.bytes_processed += size
        if is_lob:
            metrics.lob_bytes_processed += size
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, bytes):
        return "\\x" + value.hex()
    return value


def _read_lob_stream(value: Any, *, chunk_size: int) -> Any:
    chunks: list[Any] = []
    offset = 1
    while True:
        try:
            chunk = value.read(offset, chunk_size)
        except TypeError:
            chunk = value.read()
        if not chunk:
            break
        chunks.append(chunk)
        if len(chunk) < chunk_size or not _supports_offset_read(value):
            break
        offset += len(chunk)
    if not chunks:
        return b"" if _is_binary_lob_value(value) else ""
    if isinstance(chunks[0], str):
        return "".join(str(chunk) for chunk in chunks).replace("\x00", "")
    return b"".join(bytes(chunk) for chunk in chunks)


def _supports_offset_read(value: Any) -> bool:
    return value.__class__.__module__.startswith("oracledb") or hasattr(value, "size")


def _is_binary_lob_value(value: Any) -> bool:
    type_name = value.__class__.__name__.upper()
    return "BLOB" in type_name or "RAW" in type_name


def _value_size(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(str(value).encode("utf-8"))


def _failed_row_sample(
    *,
    table_name: str,
    chunk_key: str,
    row_number: int,
    key_columns: list[str],
    columns: list[str],
    row: tuple[Any, ...],
    error: Exception,
    column_name: str = "",
) -> dict[str, Any]:
    key_values: dict[str, Any] = {}
    column_index = {column.lower(): idx for idx, column in enumerate(columns)}
    for key in key_columns:
        idx = column_index.get(str(key).lower())
        if idx is not None and idx < len(row):
            key_values[key] = _safe_sample_value(row[idx])
    return {
        "table_name": table_name,
        "chunk_key": chunk_key,
        "row_number": row_number,
        "key_values": key_values,
        "column_name": column_name,
        "error": str(error),
    }


def _safe_sample_value(value: Any) -> Any:
    if value is None or isinstance(value, int | float | Decimal | bool):
        return value
    if isinstance(value, str):
        return value[:200]
    if isinstance(value, memoryview):
        return f"<memoryview bytes={len(value)}>"
    if isinstance(value, bytes | bytearray):
        return f"<binary bytes={len(value)}>"
    if hasattr(value, "read"):
        return "<lob>"
    return str(value)[:200]


def _format_failed_row_message(sample: dict[str, Any]) -> str:
    parts = [
        f"table={sample.get('table_name')}",
        f"chunk={sample.get('chunk_key') or 'full'}",
        f"row_number={sample.get('row_number')}",
    ]
    if sample.get("key_values"):
        parts.append(f"keys={sample.get('key_values')}")
    if sample.get("column_name"):
        parts.append(f"column={sample.get('column_name')}")
    parts.append(f"error={sample.get('error')}")
    return "COPY row failed: " + "; ".join(parts)
