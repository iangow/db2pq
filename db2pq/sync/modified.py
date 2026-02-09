# db2pq/sync/modified.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ModifiedInfo:
    value: str | None  # the raw “last modified” string (or None/"" if missing)
    source: str        # e.g., "wrds_comment", "sas_contents", "parquet_meta"

def is_up_to_date(*, src: str | None, dst: str | None) -> bool:
    """True if both non-empty and equal."""
    if not src or not dst:
        return False
    return src == dst

def print_update_decision(
    *,
    schema: str,
    alt_table_name: str,
    up_to_date: bool,
):
    if up_to_date:
        print(f"{schema}.{alt_table_name} already up to date.")
    else:
        print(f"Updated {schema}.{alt_table_name} is available.")
        print("Getting from WRDS.")