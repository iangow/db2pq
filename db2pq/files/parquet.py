from __future__ import annotations

import os
import re
from pathlib import Path

import ibis.selectors as s
from ibis import _
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .paths import (
    parquet_paths,
    archive_existing_parquet,
    promote_temp_parquet,
    get_pq_file,
    resolve_data_dir,
)
from .timestamps import parse_last_modified
from ..sync.modified import modified_info

def _require_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required for DataFrame outputs from pq_last_modified(). "
            "Install it with: pip install pandas "
            "or pip install \"db2pq[pandas]\""
        ) from exc
    return pd

def _normalize_timestamp_array(arr, *, source_tz: str, target_tz: str = "UTC"):
    """Convert a timestamp array to timezone-aware target_tz."""
    if not pa.types.is_timestamp(arr.type):
        return arr

    unit = arr.type.unit
    tz = arr.type.tz
    target_type = pa.timestamp(unit, tz=target_tz)

    if tz is None:
        arr = pc.assume_timezone(arr, source_tz)
        if source_tz != target_tz:
            arr = pc.cast(arr, target_type)
        return arr

    if tz != target_tz:
        arr = pc.cast(arr, target_type)

    return arr

def _normalize_timestamp_batch(batch, *, default_tz: str = "UTC"):
    """Normalize timestamp columns in a RecordBatch to timezone-aware UTC."""
    arrays = []
    fields = []

    for field, arr in zip(batch.schema, batch.columns):
        norm_arr = _normalize_timestamp_array(arr, source_tz=default_tz, target_tz="UTC")
        arrays.append(norm_arr)
        fields.append(pa.field(field.name, norm_arr.type, nullable=field.nullable, metadata=field.metadata))

    schema = pa.schema(fields, metadata=batch.schema.metadata)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)

def _normalize_timestamp_table(table, *, default_tz: str = "UTC"):
    """Normalize timestamp columns in a Table to timezone-aware UTC."""
    out = table

    for idx, field in enumerate(out.schema):
        col = out.column(idx)
        if not pa.types.is_timestamp(col.type):
            continue

        chunks = [
            _normalize_timestamp_array(chunk, source_tz=default_tz, target_tz="UTC")
            for chunk in col.chunks
        ]
        out = out.set_column(idx, field.name, pa.chunked_array(chunks))

    return out

def df_to_arrow(df, col_types=None, obs=None, batches=False):
    
    if col_types:
        types = set(col_types.values())
        for type in types:
            to_convert = [key for (key, value) in col_types.items() if value == type]
            df = df.mutate(s.across(to_convert, _.cast(type)))

    if obs is not None:
        df = df.limit(obs)

    if batches:
        return df.to_pyarrow_batches()   
    else:
        return df.to_pyarrow()
        
def get_modified_pq(file_name):
    file_path = Path(file_name).expanduser()

    if file_path.exists():
        md = pq.read_schema(file_path)
        schema_md = md.metadata
        if not schema_md:
            return ""
        if b"last_modified" in schema_md:
            return schema_md[b"last_modified"].decode("utf-8")
    return ""

def pq_archive(table_name=None, schema=None, data_dir=None, file_name=None, archive_dir=None):
    """
    Archive a parquet file by renaming it into an archive subdirectory.

    If file_name is provided, archive that exact file path.
    Otherwise, resolve the parquet file from table_name/schema/data_dir.
    Returns archived file path as a string, or None if no file was archived.
    """
    if file_name is not None:
        pq_file = Path(file_name).expanduser()
        table_basename = pq_file.stem
    else:
        if table_name is None or schema is None:
            raise ValueError("table_name and schema are required when file_name is not provided")
        pq_file = get_pq_file(table_name=table_name, schema=schema, data_dir=data_dir)
        table_basename = table_name

    comment = get_modified_pq(pq_file)
    try:
        modified_str = parse_last_modified(comment) if comment else None
    except ValueError:
        modified_str = None

    archived = archive_existing_parquet(
        pq_file,
        archive=True,
        archive_dir=archive_dir,
        table_basename=table_basename,
        modified_str=modified_str,
    )

    return str(archived) if archived is not None else None

def _restore_table_basename(name: str) -> str | None:
    """
    Recover original table basename from archived parquet stem.
    Supported suffixes: _YYYYMMDDTHHMMSSZ and _unknown_modified.
    """
    m = re.match(r"^(?P<table>.+)_(?P<suffix>\d{8}T\d{6}Z|unknown_modified)$", name)
    if not m:
        return None
    return m.group("table")

def pq_restore(file_basename, schema, data_dir=None, archive=True, archive_dir=None):
    """
    Restore an archived parquet file into the schema directory.

    file_basename may include or omit .parquet and should refer to a file in:
    <data_dir>/<schema>/<archive_dir>/
    """
    archive_dir = archive_dir or "archive"
    data_root = resolve_data_dir(data_dir)
    schema_dir = data_root / schema
    archive_path = schema_dir / archive_dir

    archived_name = Path(file_basename).name
    if not archived_name.endswith(".parquet"):
        archived_name = f"{archived_name}.parquet"

    archived_file = archive_path / archived_name
    if not archived_file.exists():
        print(f"Archived file not found: {archived_file}")
        return None

    table_basename = _restore_table_basename(archived_file.stem)
    if table_basename is None:
        print(
            f"Could not determine destination table name from archived file: {archived_file.name}"
        )
        return None

    dest_file = schema_dir / f"{table_basename}.parquet"
    archived_current = None

    if dest_file.exists():
        if archive:
            archived_current = pq_archive(file_name=dest_file, archive_dir=archive_dir)
        else:
            print(
                f"Destination file already exists ({dest_file}); set archive=True to archive it first."
            )
            return None

    try:
        archived_file.rename(dest_file)
    except Exception as exc:
        # Best-effort rollback if we archived the current destination but failed to restore.
        if archived_current:
            archived_current_path = Path(archived_current)
            if archived_current_path.exists() and not dest_file.exists():
                try:
                    archived_current_path.rename(dest_file)
                except Exception:
                    pass
        print(f"Could not restore archived parquet file: {exc}")
        return None

    return str(dest_file)

def pq_remove(
    table_name=None,
    schema=None,
    data_dir=None,
    file_name=None,
    archive=False,
    archive_dir="archive",
):
    """
    Remove a parquet file from active or archive storage.

    If file_name is provided, remove that exact file path.
    Otherwise, resolve the parquet file from table_name/schema/data_dir.
    When archive=True, resolved files are looked up under archive_dir.
    Returns removed file path as a string, or None if nothing was removed.
    """
    if file_name is not None:
        p = Path(file_name).expanduser()
    else:
        if table_name is None or schema is None:
            raise ValueError("table_name and schema are required when file_name is not provided")
        table_file = Path(table_name)
        if table_file.suffix != ".parquet":
            table_file = table_file.with_suffix(".parquet")
        data_root = resolve_data_dir(data_dir)
        if archive:
            p = data_root / schema / archive_dir / table_file.name
        else:
            p = data_root / schema / table_file.name

    if not p.exists():
        print(f"Parquet file not found: {p}")
        return None

    try:
        p.unlink()
    except Exception as exc:
        print(f"Could not remove parquet file: {exc}")
        return None

    return str(p)

def _write_tmp_parquet(
    df,
    *,
    tmp_pq_file,
    col_types=None,
    modified=None,
    obs=None,
    batched=True,
    row_group_size=1024 * 1024,
    tz: str = "UTC",
):
    """Write df to tmp_pq_file (no archiving, no promotion).

    Returns True if a temporary parquet file was written, else False.
    """
    if batched:
        batches = iter(df_to_arrow(df, col_types=col_types, obs=obs, batches=True))
        try:
            first_batch = next(batches)
        except StopIteration:
            return False
        first_batch = _normalize_timestamp_batch(
            first_batch,
            default_tz=tz,
        )

        pq_schema = first_batch.schema
        if modified:
            md = dict(pq_schema.metadata or {})
            md[b"last_modified"] = modified.encode()
            pq_schema = pq_schema.with_metadata(md)

        with pq.ParquetWriter(tmp_pq_file, pq_schema) as writer:
            writer.write_batch(first_batch)
            for batch in batches:
                batch = _normalize_timestamp_batch(
                    batch,
                    default_tz=tz,
                )
                writer.write_batch(batch)
        return True
    else:
        df_arrow = df_to_arrow(df, col_types=col_types, obs=obs)
        df_arrow = _normalize_timestamp_table(
            df_arrow,
            default_tz=tz,
        )
        if df_arrow.num_rows == 0:
            return False
        if modified:
            md = dict(df_arrow.schema.metadata or {})
            md[b"last_modified"] = modified.encode()
            df_arrow = df_arrow.replace_schema_metadata(md)
        pq.write_table(df_arrow, tmp_pq_file, row_group_size=row_group_size)
        return True

def write_parquet(
    df,
    *,
    data_dir,
    schema: str,
    table_name: str,
    col_types=None,
    modified: str | None = None,
    obs=None,
    batched: bool = True,
    row_group_size: int = 1024 * 1024,
    tz: str = "UTC",
    archive: bool = False,
    archive_dir: str | None = None,
):
    """
    End-to-end Parquet write:
      - compute pq/tmp paths
      - write tmp parquet (batched or not)
      - optionally archive existing final file
      - promote tmp -> final
    Returns Path to final parquet file, or None if no rows were selected.
    """
    _, pq_file, tmp_pq_file = parquet_paths(data_dir, schema, table_name)

    # --- existing inner logic (writes tmp_pq_file) ---
    wrote_rows = _write_tmp_parquet(
        df,
        tmp_pq_file=tmp_pq_file,
        col_types=col_types,
        modified=modified,
        obs=obs,
        batched=batched,
        row_group_size=row_group_size,
        tz=tz,
    )
    if not wrote_rows:
        print(f"No rows returned for {schema}.{table_name}; no parquet file created.")
        return None

    if archive and pq_file.exists():
        pq_archive(file_name=pq_file, archive_dir=archive_dir)

    promote_temp_parquet(tmp_pq_file, pq_file)
    return pq_file
    
def pq_last_modified_dttm(p: Path):
    """
    Return last-modified timestamp for a parquet file as a local datetime,
    or None if unavailable/unparseable.
    """
    comment = get_modified_pq(p)
    info = modified_info(kind="parquet", comment=comment)
    return info.dttm_local

def pq_last_modified_raw(p: Path):
    """
    Return last-modified timestamp for a parquet file as a local datetime,
    or None if unavailable/unparseable.
    """
    comment = get_modified_pq(p)
    info = modified_info(kind="parquet", comment=comment)
    return info.raw

def _parquet_storage(p: Path) -> str:
    """Best-effort check whether parquet bytes are available locally."""
    try:
        if not p.exists():
            return "cloud"
        if p.stat().st_size == 0:
            return "cloud"
    except OSError:
        return "cloud"
    return "local"

def _scan_row_for_parquet(p: Path, schema_name: str, *, archive: bool = False) -> dict:
    storage = _parquet_storage(p)
    comment = None

    if storage == "local":
        try:
            comment = get_modified_pq(p)
        except Exception:
            storage = "cloud"

    info = modified_info(kind="parquet", comment=comment)
    file_stem = p.stem
    table_name = file_stem
    if archive:
        table_name = _restore_table_basename(file_stem) or file_stem

    return {
        "file_name": file_stem,
        "table": table_name,
        "schema": schema_name,
        "last_mod": info.dttm_local,
        "last_mod_str": info.raw,
        "storage": storage,
    }

def pq_last_modified(
    table_name=None,
    schema=None,
    data_dir=None,
    file_name=None,
    archive=False,
    archive_dir="archive",
):
    """
    Get last-updated metadata for parquet data files.

    If file_name is provided, return metadata for that file.
    Else if table_name is provided, resolve the parquet file from
    table_name/schema/data_dir and return metadata for that file.
    Else, return a DataFrame summary for all parquet files (optionally
    constrained to a schema), including a storage indicator with values
    local/cloud. If archive=True, files are read from archive_dir.
    """
    if file_name is not None:
        return get_modified_pq(file_name)

    if table_name is not None:
        if schema is None:
            raise ValueError("schema is required when table_name is provided")
        if archive:
            pd = _require_pandas()
            data_root = resolve_data_dir(data_dir)
            scan_dir = data_root / schema / archive_dir
            requested = Path(table_name).stem
            requested_table = _restore_table_basename(requested) or requested

            rows = []
            for p in scan_dir.glob("*.parquet"):
                row = _scan_row_for_parquet(p, schema, archive=True)
                if row["table"] == requested_table:
                    rows.append(row)

            if not rows:
                return pd.DataFrame(
                    columns=["file_name", "table", "schema", "last_mod", "last_mod_str", "storage"]
                )
            return (
                pd.DataFrame(rows)
                .sort_values(["schema", "table", "file_name"])
                .reset_index(drop=True)
            )
        else:
            pq_file = get_pq_file(table_name=table_name, schema=schema, data_dir=data_dir)
            return get_modified_pq(pq_file)

    if data_dir is None:
        data_dir = os.getenv("DATA_DIR") or os.getcwd()
    data_dir = Path(os.path.expanduser(data_dir))

    rows = []

    if schema is not None:
        schema_name = schema
        subdir = data_dir / schema
        if archive:
            subdir = subdir / archive_dir
        for p in subdir.glob("*.parquet"):
            rows.append(_scan_row_for_parquet(p, schema_name, archive=archive))
    else:
        for subdir in data_dir.iterdir():
            if not subdir.is_dir():
                continue
            scan_dir = subdir / archive_dir if archive else subdir
            for p in scan_dir.glob("*.parquet"):
                rows.append(_scan_row_for_parquet(p, subdir.name, archive=archive))

    pd = _require_pandas()
    df = pd.DataFrame(rows)

    return (
        df
        .sort_values(["schema", "table", "file_name"])
        .reset_index(drop=True)
    )
