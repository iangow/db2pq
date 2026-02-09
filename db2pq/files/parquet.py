from __future__ import annotations

from pathlib import Path

import ibis.selectors as s
from ibis import _
import pyarrow as pa
import pyarrow.parquet as pq

from .paths import parquet_paths, archive_existing_parquet, promote_temp_parquet
from .timestamps import parse_last_modified  

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
        
def _infer_parquet_schema(df, *, col_types):
    from tempfile import TemporaryFile
    with TemporaryFile() as tmp:
        arrow = df_to_arrow(df, col_types=col_types, obs=10)
        pq.write_table(arrow, tmp)
        tmp.seek(0)
        return pq.read_schema(tmp)
        
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
    """Write df to tmp_pq_file (no archiving, no promotion)."""
    if batched:
        pq_schema = _infer_parquet_schema(df, col_types=col_types)
        if modified:
            pq_schema = pq_schema.with_metadata({b"last_modified": modified.encode()})

        with pq.ParquetWriter(tmp_pq_file, pq_schema) as writer:
            batches = df_to_arrow(df, col_types=col_types, obs=obs, batches=True)
            for batch in batches:
                writer.write_batch(batch)
    else:
        df_arrow = df_to_arrow(df, col_types=col_types, obs=obs)
        pq.write_table(df_arrow, tmp_pq_file, row_group_size=row_group_size)

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
    Returns Path to final parquet file.
    """
    _, pq_file, tmp_pq_file = parquet_paths(data_dir, schema, table_name)

    # --- existing inner logic (writes tmp_pq_file) ---
    _write_tmp_parquet(
        df,
        tmp_pq_file=tmp_pq_file,
        col_types=col_types,
        modified=modified,
        obs=obs,
        batched=batched,
        row_group_size=row_group_size,
    )

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