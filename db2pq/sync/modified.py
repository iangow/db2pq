from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from ..files.timestamps import last_modified_dttm, last_modified_dt

@dataclass(frozen=True)
class ModifiedInfo:
    kind: str                 # "wrds_pg", "wrds_sas", "pg", etc.
    raw: str | None           # original comment string
    dttm_local: datetime | None
    dt: date | None


def modified_info(kind: str, comment: str | None) -> ModifiedInfo:
    """
    Parse a WRDS/PG comment into a comparable ModifiedInfo.
    If comment is None/empty or unparseable, dt/dttm_local will be None.
    """
    if not comment:
        return ModifiedInfo(kind=kind, raw=comment, dttm_local=None, dt=None)

    try:
        return ModifiedInfo(
            kind=kind,
            raw=comment,
            dttm_local=last_modified_dttm(comment),
            dt=last_modified_dt(comment),
        )
    except ValueError:
        # Unknown format: treat as missing timestamp for comparison
        return ModifiedInfo(kind=kind, raw=comment, dttm_local=None, dt=None)

def update_available(*, src: ModifiedInfo, dst: ModifiedInfo) -> bool:
    """
    Return True if src is newer than dst, based on parsed dates.

    Policy (simple and safe):
    - If src.dt is missing -> cannot establish update -> False
    - If dst.dt is missing but src.dt exists -> True (assume dst unknown/old)
    - Else compare dates: src.dt > dst.dt
    """
    if src.dt is None:
        return False
    if dst.dt is None:
        return True
    return src.dt > dst.dt

def is_up_to_date(*, src: ModifiedInfo, dst: ModifiedInfo) -> bool:
    """
    Up to date if we can parse both and dst is at least as new as src.
    If src missing -> treat as not up-to-date (conservative) OR True.
    Here we use conservative: False.
    """
    if src.dt is None or dst.dt is None:
        return False
    return dst.dt >= src.dt

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
