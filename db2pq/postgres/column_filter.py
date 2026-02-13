import re
from collections.abc import Iterable
from re import Pattern


def _to_patterns(value, *, arg_name):
    if value is None:
        return []
    if isinstance(value, (str, Pattern)):
        return [value]
    if isinstance(value, Iterable):
        return list(value)
    raise TypeError(f"{arg_name} must be a regex pattern or iterable of patterns")


def _compile_patterns(patterns, *, arg_name):
    compiled = []
    for pattern in patterns:
        if isinstance(pattern, Pattern):
            compiled.append(pattern)
            continue
        try:
            compiled.append(re.compile(str(pattern)))
        except re.error as exc:
            raise ValueError(f"Invalid regex in {arg_name}: {pattern!r}") from exc
    return compiled


def filter_columns(all_cols, *, keep=None, drop=None):
    """
    Filter column names using regex patterns.

    If both are supplied, `drop` is applied first and `keep` second.
    `keep` and `drop` may be a single regex (str or compiled pattern) or
    an iterable of regex patterns.
    """
    cols = list(all_cols)

    drop_patterns = _compile_patterns(_to_patterns(drop, arg_name="drop"), arg_name="drop")
    keep_patterns = _compile_patterns(_to_patterns(keep, arg_name="keep"), arg_name="keep")

    if drop_patterns:
        cols = [c for c in cols if not any(p.search(c) for p in drop_patterns)]
    if keep_patterns:
        cols = [c for c in cols if any(p.search(c) for p in keep_patterns)]

    if not cols:
        raise ValueError("No columns selected after applying keep/drop filters")
    return cols
