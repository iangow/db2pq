from __future__ import annotations

import os
from pathlib import Path

import ibis.selectors as s
from ibis import _
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import pandas as pd

from .paths import (
    parquet_paths,
    archive_existing_parquet,
    promote_temp_parquet,
    get_pq_file,
)
from .timestamps import parse_last_modified
from ..sync.modified import modified_info

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
        modified_str = parse_last_modified(get_modified_pq(pq_file))
        archive_existing_parquet(
            pq_file,
            archive=archive,
            archive_dir=archive_dir,
            table_basename=table_name,
            modified_str=modified_str,
        )

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

def _scan_row_for_parquet(p: Path, schema_name: str) -> dict:
    storage = _parquet_storage(p)
    comment = None

    if storage == "local":
        try:
            comment = get_modified_pq(p)
        except Exception:
            storage = "cloud"

    info = modified_info(kind="parquet", comment=comment)
    return {
        "table": p.stem,
        "schema": schema_name,
        "last_mod": info.dttm_local,
        "last_mod_str": info.raw,
        "storage": storage,
    }

def pq_last_updated(table_name=None, schema=None, data_dir=None, file_name=None):
    """
    Get last-updated metadata for parquet data files.

    If file_name is provided, return metadata for that file.
    Else if table_name is provided, resolve the parquet file from
    table_name/schema/data_dir and return metadata for that file.
    Else, return a DataFrame summary for all parquet files (optionally
    constrained to a schema), including a storage indicator
    with values local/cloud.
    """
    if file_name is not None:
        return get_modified_pq(file_name)

    if table_name is not None:
        if schema is None:
            raise ValueError("schema is required when table_name is provided")
        pq_file = get_pq_file(table_name=table_name, schema=schema, data_dir=data_dir)
        return get_modified_pq(pq_file)

    if data_dir is None:
        data_dir = os.getenv("DATA_DIR") or os.getcwd()
    data_dir = Path(os.path.expanduser(data_dir))

    rows = []

    if schema is not None:
        subdir = data_dir / schema
        for p in subdir.glob("*.parquet"):
            rows.append(_scan_row_for_parquet(p, subdir.name))
    else:
        for subdir in data_dir.iterdir():
            if not subdir.is_dir():
                continue
            for p in subdir.glob("*.parquet"):
                rows.append(_scan_row_for_parquet(p, subdir.name))

    df = pd.DataFrame(rows)

    return (
        df
        .sort_values(["schema", "table"])
        .reset_index(drop=True)
    )
