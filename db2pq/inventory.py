"""Machine-readable views of a local Parquet repository.

These helpers answer two questions without pandas, without a database
connection and without printing: what is in ``DATA_DIR``, and which of the
tables a project needs are missing. They return plain dictionaries so the
result can be dumped as JSON by scripts and coding agents.
"""
from __future__ import annotations

import os
from pathlib import Path

from .files.paths import resolve_data_dir


def _split_table(spec) -> tuple[str, str]:
    """Accept ``"crsp.dsf"`` or ``("dsf", "crsp")`` and return (schema, table)."""
    if isinstance(spec, str):
        schema, sep, table = spec.partition(".")
        if not sep or not schema or not table:
            raise ValueError(
                f"Table {spec!r} must be written as 'schema.table', e.g. 'crsp.dsf'."
            )
        return schema, table
    table, schema = spec[0], spec[1]
    return str(schema), str(table)


def _describe(path: Path, schema: str, table: str) -> dict:
    row = {
        "schema": schema,
        "table": table,
        "path": str(path),
        "exists": path.is_file(),
        "size_bytes": None,
        "n_rows": None,
        "n_columns": None,
        "last_modified": None,
        "error": None,
    }
    if not row["exists"]:
        return row
    try:
        import pyarrow.parquet as pq

        row["size_bytes"] = path.stat().st_size
        meta = pq.ParquetFile(path)
        row["n_rows"] = meta.metadata.num_rows
        row["n_columns"] = meta.metadata.num_columns
        kv = meta.schema_arrow.metadata or {}
        raw = kv.get(b"last_modified")
        row["last_modified"] = raw.decode("utf-8") if raw else None
    except Exception as exc:  # unreadable or partially synced file
        row["error"] = f"{type(exc).__name__}: {exc}"
    return row


def data_dir_info(data_dir=None) -> dict:
    """Report which data directory db2pq will use and why.

    Returns a dictionary with ``data_dir`` (resolved path), ``source``
    (``"argument"``, ``"DATA_DIR"`` or ``"cwd"``) and ``exists``. A source of
    ``"cwd"`` means ``DATA_DIR`` is not set and Parquet files would be written
    under the current working directory.
    """
    if data_dir is not None:
        source = "argument"
    else:
        resolved_probe = resolve_data_dir(None)  # loads .env as a side effect
        source = "DATA_DIR" if os.getenv("DATA_DIR") else "cwd"
        del resolved_probe
    resolved = resolve_data_dir(data_dir)
    return {"data_dir": str(resolved), "source": source, "exists": resolved.is_dir()}


def pq_inventory(schema=None, *, data_dir=None) -> list[dict]:
    """List the Parquet files in a data directory with their metadata.

    Parameters
    ----------
    schema : str, optional
        Restrict the listing to one schema directory.
    data_dir : str, optional
        Root directory of the Parquet data repository. If omitted, use
        ``DATA_DIR`` or the current working directory.

    Returns
    -------
    list[dict]
        One dictionary per file with keys ``schema``, ``table``, ``path``,
        ``exists``, ``size_bytes``, ``n_rows``, ``n_columns``,
        ``last_modified`` (the string embedded by db2pq, or ``None``) and
        ``error`` (set when the file could not be read). Sorted by schema and
        table. Archive directories are not included.

    Examples
    --------
    >>> from db2pq import pq_inventory
    >>> pq_inventory("crsp")
    """
    root = resolve_data_dir(data_dir)
    if not root.is_dir():
        return []
    schema_dirs = [root / schema] if schema else sorted(
        p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")
    )
    rows = []
    for schema_dir in schema_dirs:
        for path in sorted(schema_dir.glob("*.parquet")):
            if path.name.startswith(".temp_"):
                continue
            rows.append(_describe(path, schema_dir.name, path.stem))
    return rows


def pq_check(tables, *, data_dir=None) -> dict:
    """Check that the tables a project needs are present.

    Parameters
    ----------
    tables : iterable
        Table names written as ``"schema.table"`` (``"crsp.dsf"``) or as
        ``(table, schema)`` pairs, the argument order of ``wrds_update_pq``.
    data_dir : str, optional
        Root directory of the Parquet data repository.

    Returns
    -------
    dict
        ``data_dir``, ``ok`` (``True`` when nothing is missing or unreadable),
        ``present`` and ``missing`` (lists of ``"schema.table"``), and
        ``tables`` (the per-table dictionaries of ``pq_inventory``). Nothing
        is created on disk.

    Examples
    --------
    >>> from db2pq import pq_check
    >>> pq_check(["crsp.dsf", "comp.funda"])["missing"]
    """
    root = resolve_data_dir(data_dir)
    rows = []
    for spec in tables:
        schema, table = _split_table(spec)
        rows.append(_describe(root / schema / f"{table}.parquet", schema, table))
    present = [f"{r['schema']}.{r['table']}" for r in rows if r["exists"] and not r["error"]]
    missing = [f"{r['schema']}.{r['table']}" for r in rows if not (r["exists"] and not r["error"])]
    return {
        "data_dir": str(root),
        "ok": not missing,
        "present": present,
        "missing": missing,
        "tables": rows,
    }


__all__ = ["data_dir_info", "pq_check", "pq_inventory"]
