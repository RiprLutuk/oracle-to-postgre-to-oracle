from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_inventory_xlsx(path: Path, inventory_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(inventory_rows).to_excel(path, index=False, sheet_name="inventory")


def write_rows_xlsx(path: Path, rows: list[dict], *, sheet_name: str = "rows") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_excel(path, index=False, sheet_name=sheet_name[:31])
