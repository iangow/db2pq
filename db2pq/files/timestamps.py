import re
from zoneinfo import ZoneInfo
from datetime import datetime, time  # <-- import datetime.time here

_NY = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")

_UPDATED_RE = re.compile(r"\(Updated\s+(\d{4}-\d{2}-\d{2})\)\s*$")


def parse_last_modified(s: str) -> str:
    """
    Return a filename-safe UTC timestamp (YYYYMMDDTHHMMSSZ) from either:
      1) 'Last modified: 11/26/2025 01:40:41'  (America/New_York local time)
      2) '... (Updated 2026-01-07)'            (assume 02:00 America/New_York)

    Raises ValueError if no known pattern matches.
    """
    dt_local = last_modified_dttm(s)
    dt_utc = dt_local.astimezone(_UTC)
    return dt_utc.strftime("%Y%m%dT%H%M%SZ")


def last_modified_dttm(s: str) -> datetime:
    s = s.strip()

    # Case 1: "Last modified: ..."
    if s.startswith("Last modified:"):
        ts = s.removeprefix("Last modified:").strip()
        return datetime.strptime(ts, "%m/%d/%Y %H:%M:%S").replace(tzinfo=_NY)

    # Case 2: "... (Updated yyyy-mm-dd)"
    m = _UPDATED_RE.search(s)
    if not m:
        raise ValueError(f"Unrecognized timestamp format: {s!r}")

    d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
    return datetime.combine(d, time(2, 0, 0), tzinfo=_NY)


def last_modified_dt(s: str):
    return last_modified_dttm(s).date()
