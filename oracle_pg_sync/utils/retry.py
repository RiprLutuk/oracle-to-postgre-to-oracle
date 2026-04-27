from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    should_retry: Callable[[Exception], bool] | None = None,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts or (should_retry and not should_retry(exc)):
                raise
            time.sleep(delay_seconds)
    raise RuntimeError("retry exhausted") from last_error
