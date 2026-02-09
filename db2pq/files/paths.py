from __future__ import annotations
import os
from pathlib import Path

def resolve_data_dir(data_dir: str | Path | None = None) -> Path:
    if data_dir is None:
        data_dir = os.getenv("DATA_DIR") or os.getcwd()
    return Path(os.path.expanduser(data_dir)).expanduser()

def get_pq_file(table_name, schema, *, data_dir=None):
    data_dir = resolve_data_dir(data_dir)

    schema_dir = data_dir / schema
    schema_dir.mkdir(parents=True, exist_ok=True)

    return (schema_dir / table_name).with_suffix(".parquet")

def get_pq_files(schema, *, data_dir=None):
    """Get a list of parquet files in a schema.

    Parameters
    ----------
    schema: 
        Name of database schema.
            
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    Returns
    -------
    pq_files: [string]
        Names of parquet files found.
    """
    data_dir = resolve_data_dir(data_dir)

    pq_dir = data_dir / schema
    return [p.stem for p in pq_dir.glob("*.parquet")]

def parquet_paths(data_dir: Path, schema: str, table_basename: str):
    """
    Return (pq_dir, pq_file, tmp_pq_file) and ensure pq_dir exists.
    """
    pq_dir = data_dir / schema
    pq_dir.mkdir(parents=True, exist_ok=True)

    pq_file = pq_dir / f"{table_basename}.parquet"
    tmp_pq_file = pq_dir / f".temp_{table_basename}.parquet"
    return pq_dir, pq_file, tmp_pq_file


def archive_existing_parquet(
    pq_file: Path,
    *,
    archive: bool,
    archive_dir: str | None,
    table_basename: str,
    modified_str: str | None,
):
    """
    If archive is True and pq_file exists, move it into archive_dir with a suffix.
    """
    if not archive or not pq_file.exists():
        return None

    archive_dir = archive_dir or "archive"
    archive_path = pq_file.parent / archive_dir
    archive_path.mkdir(parents=True, exist_ok=True)

    # If modified_str is missing, still archive deterministically
    suffix = modified_str or "unknown_modified"
    pq_file_archive = archive_path / f"{table_basename}_{suffix}.parquet"
    pq_file.rename(pq_file_archive)
    return pq_file_archive


def promote_temp_parquet(tmp_pq_file: Path, pq_file: Path):
    """
    Replace/rename tmp to final.
    """
    tmp_pq_file.rename(pq_file)
    return pq_file