from __future__ import annotations

import os
from pathlib import Path

import ibis.selectors as s
from ibis import _
import pyarrow.parquet as pq

import pandas as pd

from .paths import parquet_paths, archive_existing_parquet, promote_temp_parquet
from .timestamps import parse_last_modified
from ..sync.modified import modified_info

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
    file_path = Path(file_name)

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

        pq_schema = first_batch.schema
        if modified:
            md = dict(pq_schema.metadata or {})
            md[b"last_modified"] = modified.encode()
            pq_schema = pq_schema.with_metadata(md)

        with pq.ParquetWriter(tmp_pq_file, pq_schema) as writer:
            writer.write_batch(first_batch)
            for batch in batches:
                writer.write_batch(batch)
        return True
    else:
        df_arrow = df_to_arrow(df, col_types=col_types, obs=obs)
        if df_arrow.num_rows == 0:
            return False
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

def pq_last_updated(data_dir=None, schema=None):
    """
    Get last-updated metadata for parquet data files.
    """
    if data_dir is None:
        data_dir = os.getenv("DATA_DIR") or os.getcwd()
    data_dir = Path(os.path.expanduser(data_dir))

    rows = []

    if schema is not None:
        subdir = data_dir / schema
        for p in subdir.glob("*.parquet"):
            rows.append({
                "table": p.stem,
                "schema": subdir.name,
                "last_mod": pq_last_modified_dttm(p),
                "last_mod_str": pq_last_modified_raw(p),
            })
    else:
        for subdir in data_dir.iterdir():
            if not subdir.is_dir():
                continue
            for p in subdir.glob("*.parquet"):
                rows.append({
                    "table": p.stem,
                    "schema": subdir.name,
                    "last_mod": pq_last_modified_dttm(p),
                    "last_mod_str": pq_last_modified_raw(p),
                })

    df = pd.DataFrame(rows)

    return (
        df
        .sort_values(["schema", "table"])
        .reset_index(drop=True)
    )
