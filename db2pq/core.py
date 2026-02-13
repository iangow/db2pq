import os
from time import gmtime, strftime

from .sas.stream import get_modified_str
from .files.paths import get_pq_file, get_pq_files
from .files.parquet import write_parquet, get_modified_pq
from .postgres.duckdb_pg import read_postgres_table
from .postgres.comments import get_wrds_comment
from .postgres.schema import db_schema_tables
from .sync.modified import is_up_to_date
from .sync.modified import modified_info, update_available
from .postgres._defaults import resolve_pg_connection

def db_to_pq(
    table_name,
    schema,
    *,
    user=None,
    host=None,
    database=None,
    port=None,
    data_dir=None,
    col_types=None,
    row_group_size=1048576,
    obs=None,
    modified=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    batched=True,
    threads=None,
    archive=False,
    archive_dir=None,
):
    """Export a PostgreSQL table to a parquet file.

    Parameters
    ----------
    table_name: 
        Name of table in database.
    
    schema: 
        Name of database schema.

    host: string [Optional]
        Host name for the PostgreSQL server.
        The default is to use the environment value `PGHOST`.

    database: string [Optional]
        Name for the PostgreSQL database.
        The default is to use the environment value `PGDATABASE`
        or user.
            
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    col_types: Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to
        PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is
        handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by PostgreSQL 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno':'integer', 'permco':'integer'}`.
    
    row_group_size: int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs: Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `db_to_pq()` with large tables.

    modified: string [Optional]
        Last modified string.
        
    alt_table_name: string [Optional]
        Basename of parquet file. Used when file should have different name from 
        `table_name`.

    keep: string or iterable [Optional]
        Regex pattern(s) indicating columns to keep.
        
    drop: string or iterable [Optional]
        Regex pattern(s) indicating columns to drop.
        If both `drop` and `keep` are provided, `drop` is applied first.
    
    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
        
    threads: int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> db_to_pq("dsi", "crsp")
    >>> db_to_pq("feed21_bankruptcy_notification", "audit")
    """
    
    user, host, dbname, port = resolve_pg_connection(
        user=user, host=host, dbname=database, port=port
    )
    
    if not alt_table_name:
        alt_table_name = table_name

    df = read_postgres_table(
        user=user,
        host=host,
        port=port,
        database=dbname,
        schema=schema,
        table_name=table_name,
        threads=threads,
        keep=keep,
        drop=drop,
    )
        
    pq_file = write_parquet(
        df,
        data_dir=data_dir,
        schema=schema,
        table_name=alt_table_name,
        col_types=col_types,
        modified=modified,
        obs=obs,
        batched=batched,
        row_group_size=row_group_size,
        archive=archive,
        archive_dir=archive_dir,
    )
    
    return str(pq_file) if pq_file is not None else None

def wrds_pg_to_pq(
    table_name,
    schema,
    *,
    wrds_id=None,
    data_dir=None,
    col_types=None,
    row_group_size=1048576,
    obs=None,
    modified=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    batched=True,
    threads=3,
    archive=False,
    archive_dir=None,
):
    """Export a table from the WRDS PostgreSQL database to a parquet file.

    Parameters
    ----------
    table_name: 
        Name of table in database.
    
    schema: 
        Name of database schema.

    wrds_id : string
        WRDS user ID used to access WRDS services.
        This parameter is required and must be provided either explicitly
        or via the `WRDS_ID` environment variable.

    data_dir : string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    col_types : Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by PostgreSQL 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno': 'int32', 'permco': 'int32'}`.
    
    row_group_size : int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs : Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `wrds_pg_to_pq()` with large tables.
    
    alt_table_name : string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    keep : string or iterable [Optional]
        Regex pattern(s) indicating columns to keep.
        
    drop : string or iterable [Optional]
        Regex pattern(s) indicating columns to drop.
        If both `drop` and `keep` are provided, `drop` is applied first.

    batched : bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
    
    threads : int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> wrds_pg_to_pq("dsi", "crsp")
    >>> wrds_pg_to_pq("feed21_bankruptcy_notification", "audit")
    """
    if wrds_id is None:
        wrds_id = os.getenv("WRDS_ID")
        if not wrds_id:
            raise ValueError(
                "wrds_id must be provided either as an argument or "
                "via the WRDS_ID environment variable"
            )
    
    return db_to_pq(
        table_name,
        schema,
        user=wrds_id,
        host="wrds-pgdata.wharton.upenn.edu",
        database="wrds",
        port=9737,
        data_dir=data_dir,
        col_types=col_types,
        row_group_size=row_group_size,
        obs=obs,
        modified=modified,
        alt_table_name=alt_table_name,
        keep=keep,
        drop=drop,
        batched=batched,
        threads=threads,
        archive=archive,
        archive_dir=archive_dir,
    )

    
def db_schema_to_pq(
    schema: str,
    *,
    user: str | None = None,
    host: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
    data_dir: str | None = None,
    row_group_size: int = 1024 * 1024,
    batched: bool = True,
    threads: int | None = None,
    archive: bool = False,
    archive_dir: str | None = None,
) -> list[str]:
    """Export all tables in a PostgreSQL schema to Parquet files.

    Parameters
    ----------
    schema : str
        Name of the PostgreSQL database schema.

    user : str, optional
        PostgreSQL user role.
        If not provided, defaults to the value of the `PGUSER`
        environment variable, or (if unset) the current system user.

    host : str, optional
        Host name for the PostgreSQL server.
        If not provided, defaults to the value of the `PGHOST`
        environment variable, or `"localhost"` if unset.

    dbname : str, optional
        Name of the PostgreSQL database.
        If not provided, defaults to the value of the `PGDATABASE`
        environment variable, or (if unset) the resolved `user`.

    port : int, optional
        Port for the PostgreSQL server.
        If not provided, defaults to the value of the `PGPORT`
        environment variable, or `5432` if unset.

    data_dir : str, optional
        Root directory of the Parquet data repository.
        If not provided, defaults to the value of the `DATA_DIR`
        environment variable, or the current working directory.

    row_group_size : int, optional
        Maximum number of rows in each written Parquet row group.
        Must be positive. Default is ``1024 * 1024``.

    batched : bool, optional
        Whether data are extracted in batches using
        ``to_pyarrow_batches()`` instead of a single call to
        ``to_pyarrow()``. Using batches reduces memory usage for
        large tables at the cost of slightly lower performance.

    threads : int, optional
        Number of threads DuckDB is allowed to use.
        If provided, must be positive.

    archive : bool, optional
        Whether an existing Parquet file should be archived before
        being replaced.

    archive_dir : str, optional
        Name of the directory (relative to ``data_dir/schema``)
        where archived Parquet files will be stored.

    Returns
    -------
    results : list[str]
        List of Parquet file paths returned by ``db_to_pq()``,
        one for each table in the schema.

    Examples
    ----------
    >>> db_schema_to_pq("crsp")
    >>> db_schema_to_pq("audit", archive=True)
    """
    if row_group_size <= 0:
        raise ValueError("row_group_size must be positive")

    if threads is not None and threads <= 0:
        raise ValueError("threads must be positive or None")

    tables = db_schema_tables(
        schema,
        user=user,
        host=host,
        dbname=dbname,
        port=port,
    )

    results: list[str] = []
    for table_name in tables:
        results.append(
            db_to_pq(
                table_name=table_name,
                schema=schema,
                user=user,
                host=host,
                database=database,
                port=port,
                data_dir=data_dir,
                row_group_size=row_group_size,
                threads=threads,
                batched=batched,
                archive=archive,
                archive_dir=archive_dir,
            )
        )

    return results

def wrds_update_pq(
    table_name,
    schema,
    *,
    wrds_id=None,
    data_dir=None,
    force=False,
    col_types=None,
    encoding="utf-8",
    sas_schema=None,
    row_group_size=1048576,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    batched=True,
    threads=3,
    use_sas=False,
    archive=False,
    archive_dir=None,
):
    """Export a table from the WRDS PostgreSQL database to a Parquet file.

    Parameters
    ----------
    table_name :
        Name of the table in the WRDS PostgreSQL database.

    schema :
        Name of the database schema.

    wrds_id : string
        WRDS user ID used to access WRDS services.
        This parameter is required and must be provided either explicitly
        or via the `WRDS_ID` environment variable.

    data_dir : string, optional
        Root directory of the Parquet data repository.
        If not provided, defaults to the value of the `DATA_DIR`
        environment variable, or the current working directory.
        
    force: Boolean
        Whether update should proceed regardless of date comparison results.
    
    col_types: Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by PostgreSQL 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno': 'int32', 'permco': 'int32'}`.
    
    row_group_size: int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs: Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `wrds_update_pq()` with large tables.
    
    alt_table_name: string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    keep: string or iterable [Optional]
        Regex pattern(s) indicating columns to keep.
        
    drop: string or iterable [Optional]
        Regex pattern(s) indicating columns to drop.
        If both `drop` and `keep` are provided, `drop` is applied first.

    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
                
    threads: int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    use_sas: bool [Optional]
        Should update get table comments from SAS data file.
        If False, then updated string comes from WRDS PostgreSQL table comment.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> wrds_update_pq("dsi", "crsp")
    >>> wrds_update_pq("feed21_bankruptcy_notification", "audit")
    """                       
    if wrds_id is None:
        wrds_id = os.getenv("WRDS_ID")
        if not wrds_id:
            raise ValueError(
                "wrds_id must be provided either as an argument or "
                "via the WRDS_ID environment variable"
            )
        
    if not sas_schema:
        sas_schema = schema

    if not alt_table_name:
        alt_table_name = table_name
                
    if use_sas:
        wrds_comment = get_modified_str(
            table_name=table_name, sas_schema=sas_schema, wrds_id=wrds_id, 
            encoding=encoding
        )
    else:
        wrds_comment = get_wrds_comment(
            table_name=table_name, schema=schema, wrds_id=wrds_id)
           
    pq_file = get_pq_file(table_name=table_name, schema=schema, data_dir=data_dir)
    pq_comment = get_modified_pq(pq_file)
    wrds_mod = modified_info("wrds_pg", wrds_comment)
    pq_mod   = modified_info("pq", pq_comment)
    
    if force:
        print("Forcing update based on user request.")
    elif not update_available(src=wrds_mod, dst=pq_mod):
        print(f"{schema}.{alt_table_name} already up to date.")
        return
    else:
        print(f"Updated {schema}.{alt_table_name} is available.")
        print(f"Beginning file download at {get_now()} UTC.")

    pq_file = wrds_pg_to_pq(table_name=table_name,
                            schema=schema,
                            data_dir=data_dir,
                            wrds_id=wrds_id,
                            col_types=col_types,
                            row_group_size=row_group_size,
                            obs=obs,
                            modified=wrds_comment,
                            alt_table_name=alt_table_name,
                            keep=keep,
                            drop=drop,
                            batched=batched,
                            threads=threads,
                            archive=archive,
                            archive_dir=archive_dir)
    if pq_file is None:
        print(f"No file download completed at {get_now()} UTC (no rows returned).")
    else:
        print(f"Completed file download at {get_now()} UTC.")
    
def get_now():
    return strftime("%Y-%m-%d %H:%M:%S", gmtime())
            
def wrds_update_schema(schema, *, data_dir=None, threads=3, archive=False):
    """Update existing parquet files in a schema.

    Parameters
    ----------
    schema : str
        Name of database schema.

    data_dir : str, optional
        Root directory of parquet data repository.
        If not provided, defaults to the value of the `DATA_DIR`
        environment variable, or the current directory.

    threads : int, optional
        The number of threads DuckDB is allowed to use.

    archive : bool, optional
        Whether any existing parquet file will be archived.

    Returns
    -------
    pq_files : list[str]
        Names of parquet files updated.
    """
    pq_files = get_pq_files(schema=schema, data_dir=data_dir)

    for pq_file in pq_files:
        wrds_update_pq(
            table_name=pq_file,
            schema=schema,
            data_dir=data_dir,
            threads=threads,
            archive=archive,
        )

    return pq_files                      
