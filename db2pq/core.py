from time import gmtime, strftime
from pathlib import Path


_MODIFIED_DEFAULT = object()


def _resolve_numeric_mode(engine: str, numeric_mode: str | None) -> str | None:
    if numeric_mode is None:
        return "text" if engine == "adbc" else None

    if numeric_mode not in {"text", "float64", "decimal"}:
        raise ValueError("numeric_mode must be one of 'text', 'float64', or 'decimal'")

    return numeric_mode


def _resolve_default_table_modified(
    *,
    modified: str | None,
    table_name: str,
    schema: str,
    user: str,
    host: str,
    database: str,
    port: int,
) -> str | None:
    if modified is not None:
        return modified

    from .postgres.comments import get_pg_comment

    return get_pg_comment(
        table_name=table_name,
        schema=schema,
        user=user,
        host=host,
        dbname=database,
        port=port,
    )


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
    where=None,
    batched=True,
    threads=None,
    tz="UTC",
    engine=None,
    numeric_mode=None,
    adbc_batch_size_hint_bytes=None,
    adbc_use_copy=None,
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
        Last modified string to embed in parquet metadata. If omitted, use
        the source PostgreSQL table comment as parquet ``last_modified``
        metadata when available.
        
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

    engine : {"duckdb", "adbc"} [Optional]
        Query execution engine used to read PostgreSQL data before writing
        Parquet. ``"adbc"`` streams Arrow record batches directly from
        PostgreSQL.

    numeric_mode : {"text", "float64", "decimal"} or None [Optional]
        Handling for PostgreSQL ``NUMERIC`` columns. If ``None`` (default),
        each engine uses its least-interfering behavior: DuckDB preserves
        native decimal output, while ADBC defaults to text-backed numerics.
        ``"text"`` casts numerics to ``TEXT`` and ``"float64"`` casts them
        to ``DOUBLE PRECISION`` on both engines. ``"decimal"`` preserves
        DuckDB decimals and, on the ADBC path, transports eligible values as
        text before converting them back to Arrow decimals using PostgreSQL
        precision/scale metadata. Explicit ``col_types`` entries take
        precedence.

    adbc_batch_size_hint_bytes : int [Optional]
        On the ADBC path, hint the PostgreSQL ADBC driver about the desired
        Arrow batch size in bytes. This can affect throughput by changing the
        size of batches returned by ``fetch_record_batch()``.

    adbc_use_copy : bool [Optional]
        On the ADBC path, enable or disable the PostgreSQL driver's ``COPY``
        optimization explicitly. If omitted, the driver default is used.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> db_to_pq("dsi", "crsp")
    >>> db_to_pq("feed21_bankruptcy_notification", "audit")
    """
    from .config import get_default_engine
    from .files.parquet import write_parquet
    from .postgres._defaults import resolve_pg_connection
    from .postgres.adbc import export_postgres_table_via_adbc
    from .postgres.duckdb_pg import read_postgres_table
    
    user, host, dbname, port = resolve_pg_connection(
        user=user, host=host, dbname=database, port=port
    )
    
    if not alt_table_name:
        alt_table_name = table_name

    if engine is None:
        engine = get_default_engine()

    engine = engine.lower()
    if engine not in {"duckdb", "adbc"}:
        raise ValueError("engine must be either 'duckdb' or 'adbc'")
    numeric_mode = _resolve_numeric_mode(engine, numeric_mode)
    modified = _resolve_default_table_modified(
        modified=modified,
        table_name=table_name,
        schema=schema,
        user=user,
        host=host,
        database=dbname,
        port=port,
    )

    uri = f"postgresql://{user}@{host}:{port}/{dbname}"

    if engine == "adbc":
        from .files.parquet import pq_archive
        from .files.paths import parquet_paths, promote_temp_parquet

        _, pq_file, tmp_pq_file = parquet_paths(data_dir, schema, alt_table_name)
        pq_file = Path(pq_file)
        tmp_pq_file = Path(tmp_pq_file)

        pq_result = export_postgres_table_via_adbc(
            uri=uri,
            schema=schema,
            table_name=table_name,
            out_file=tmp_pq_file,
            col_types=col_types,
            modified=modified,
            obs=obs,
            keep=keep,
            drop=drop,
            where=where,
            row_group_size=row_group_size,
            tz=tz,
            numeric_mode=numeric_mode,
            adbc_batch_size_hint_bytes=adbc_batch_size_hint_bytes,
            adbc_use_copy=adbc_use_copy,
        )
        if pq_result is None:
            print(f"No rows returned for {schema}.{alt_table_name}; no parquet file created.")
            return None

        if archive and pq_file.exists():
            pq_archive(file_name=pq_file, archive_dir=archive_dir)

        promote_temp_parquet(tmp_pq_file, pq_file)
        return str(pq_file)

    df = read_postgres_table(
        user=user,
        host=host,
        port=port,
        database=dbname,
        schema=schema,
        table_name=table_name,
        col_types=col_types,
        obs=obs,
        threads=threads,
        keep=keep,
        drop=drop,
        where=where,
        tz=tz,
        numeric_mode=numeric_mode,
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
        tz=tz,
        archive=archive,
        archive_dir=archive_dir,
        total_rows=getattr(df, "total_rows", None),
        progress_label=getattr(df, "progress_label", f"{schema}.{alt_table_name}"),
    )
    
    return str(pq_file) if pq_file is not None else None

def db_to_pg(
    table_name,
    schema,
    *,
    user=None,
    host=None,
    database=None,
    port=None,
    dst_user=None,
    dst_host=None,
    dst_database=None,
    dst_port=None,
    dst_schema=None,
    col_types=None,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    tz="UTC",
    create_roles=True,
):
    """Write a PostgreSQL table to another PostgreSQL database.

    Parameters
    ----------
    table_name : str
        Name of the source PostgreSQL table.

    schema : str
        Name of the source PostgreSQL schema.

    user, host, database, port : optional
        Connection settings for the source PostgreSQL database. If omitted,
        resolve from the same environment/default chain used by
        ``db_to_pq()``.

    dst_user, dst_host, dst_database, dst_port : optional
        Connection settings for the destination PostgreSQL database. If
        omitted, resolve from the same environment/default chain used for
        other destination PostgreSQL helpers.

    dst_schema : str, optional
        Destination PostgreSQL schema. If omitted, defaults to ``schema``.

    col_types : dict, optional
        Explicit destination PostgreSQL column types for selected columns.

    obs : int, optional
        Number of rows to copy from the source table. Implemented with SQL
        ``LIMIT``.

    alt_table_name : str, optional
        Destination PostgreSQL table name. If omitted, defaults to
        ``table_name``.

    keep, drop : str or iterable, optional
        Regex pattern(s) describing columns to keep or drop before loading
        the destination table. If both are supplied, ``drop`` is applied
        first.

    tz : str, optional
        Default timezone used when normalizing timestamp columns.

    create_roles : bool, optional
        If ``True``, ensure destination schema owner and access roles exist
        and apply ownership and grants to the loaded table.

    Returns
    -------
    bool
        ``True`` after the destination table has been created or replaced.

    Examples
    ----------
    >>> db_to_pg("dsi", "crsp", dst_database="research")
    >>> db_to_pg("company", "comp", dst_schema="comp_mirror", obs=1000)
    """
    from .postgres._defaults import resolve_pg_connection
    from .postgres.update import postgres_write_pg

    user, host, dbname, port = resolve_pg_connection(
        user=user, host=host, dbname=database, port=port
    )
    dst_user, dst_host, dst_dbname, dst_port = resolve_pg_connection(
        user=dst_user, host=dst_host, dbname=dst_database, port=dst_port
    )

    src_uri = f"postgresql://{user}@{host}:{port}/{dbname}"
    dst_uri = f"postgresql://{dst_user}@{dst_host}:{dst_port}/{dst_dbname}"

    return postgres_write_pg(
        table_name=table_name,
        schema=schema,
        src_uri=src_uri,
        dst_uri=dst_uri,
        dst_schema=dst_schema,
        col_types=col_types,
        obs=obs,
        alt_table_name=alt_table_name,
        keep=keep,
        drop=drop,
        create_roles=create_roles,
        tz=tz,
    )

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
    where=None,
    batched=True,
    threads=3,
    tz="UTC",
    engine=None,
    numeric_mode=None,
    adbc_batch_size_hint_bytes=None,
    adbc_use_copy=None,
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

    modified : string [Optional]
        Last modified string to embed in parquet metadata. If omitted, use
        the WRDS PostgreSQL table comment as parquet ``last_modified``
        metadata when available.
    
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

    engine : {"duckdb", "adbc"} [Optional]
        Query execution engine used to read PostgreSQL data before writing
        Parquet.

    numeric_mode : {"text", "float64", "decimal"} or None [Optional]
        Handling for PostgreSQL ``NUMERIC`` columns. ``None`` keeps the
        engine-specific default: native decimals on DuckDB, text-backed
        numerics on ADBC. Explicit ``col_types`` entries take precedence.

    adbc_batch_size_hint_bytes : int [Optional]
        On the ADBC path, hint the PostgreSQL ADBC driver about the desired
        Arrow batch size in bytes.

    adbc_use_copy : bool [Optional]
        On the ADBC path, enable or disable the PostgreSQL driver's ``COPY``
        optimization explicitly.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> wrds_pg_to_pq("dsi", "crsp")
    >>> wrds_pg_to_pq("feed21_bankruptcy_notification", "audit")
    """
    from .credentials import ensure_wrds_access
    from .postgres.comments import get_pg_conn
    from .postgres.introspect import table_exists

    wrds_id = ensure_wrds_access(wrds_id)
    uri = f"postgresql://{wrds_id}@wrds-pgdata.wharton.upenn.edu:9737/wrds"

    with get_pg_conn(uri) as conn:
        if not table_exists(conn, schema, table_name):
            print(f"Table with name {table_name} does not exist.")
            return None
    
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
        where=where,
        batched=batched,
        threads=threads,
        tz=tz,
        engine=engine,
        numeric_mode=numeric_mode,
        adbc_batch_size_hint_bytes=adbc_batch_size_hint_bytes,
        adbc_use_copy=adbc_use_copy,
        archive=archive,
        archive_dir=archive_dir,
    )


def wrds_sql_to_pq(
    sql,
    table_name,
    schema,
    *,
    wrds_id=None,
    data_dir=None,
    row_group_size=1048576,
    modified=None,
    alt_table_name=None,
    threads=3,
    tz="UTC",
    engine=None,
    adbc_batch_size_hint_bytes=None,
    adbc_use_copy=None,
    archive=False,
    archive_dir=None,
):
    """Run a SQL query against WRDS PostgreSQL and write the result to Parquet.

    Parameters
    ----------
    sql :
        SQL query to execute against the WRDS PostgreSQL database.

    table_name :
        Logical source table name used for the output parquet basename unless
        ``alt_table_name`` is supplied.

    schema :
        Schema name used for the output parquet directory layout.

    wrds_id : string [Optional]
        WRDS user ID used to access WRDS services. This parameter is required
        and must be provided either explicitly or via the ``WRDS_ID``
        environment variable.

    data_dir : string [Optional]
        Root directory of parquet data repository.

    row_group_size : int [Optional]
        Maximum number of rows in each written row group.

    modified : string [Optional]
        Last modified string to embed in parquet metadata.

    alt_table_name : string [Optional]
        Basename of parquet file. Used when the file should have a different
        name than ``table_name``.

    threads : int [Optional]
        Maximum DuckDB worker threads to use when ``engine="duckdb"``.

    tz : string [Optional]
        Time zone assumption for naive PostgreSQL timestamps before normalizing
        parquet output to UTC.

    engine : {"duckdb", "adbc"} [Optional]
        Query execution engine used to run the WRDS PostgreSQL SQL.

    adbc_batch_size_hint_bytes : int [Optional]
        ADBC batch size hint in bytes when ``engine="adbc"``.

    adbc_use_copy : bool [Optional]
        Explicitly enable or disable the PostgreSQL ADBC driver's ``COPY``
        optimization when ``engine="adbc"``.

    Returns
    -------
    pq_file : string
        Name of parquet file created.
    """
    from pathlib import Path

    from .config import get_default_engine
    from .credentials import ensure_wrds_access
    from .files.parquet import pq_archive, write_record_batch_reader_to_parquet
    from .files.paths import parquet_paths, promote_temp_parquet
    from .postgres.adbc import export_postgres_query_via_adbc
    from .postgres.duckdb_pg import read_postgres_query

    wrds_id = ensure_wrds_access(wrds_id)
    uri = f"postgresql://{wrds_id}@wrds-pgdata.wharton.upenn.edu:9737/wrds"

    if not alt_table_name:
        alt_table_name = table_name

    if engine is None:
        engine = get_default_engine()

    engine = engine.lower()
    if engine not in {"duckdb", "adbc"}:
        raise ValueError("engine must be either 'duckdb' or 'adbc'")

    _, pq_file, tmp_pq_file = parquet_paths(data_dir, schema, alt_table_name)
    pq_file = Path(pq_file)
    tmp_pq_file = Path(tmp_pq_file)

    if engine == "adbc":
        result = export_postgres_query_via_adbc(
            uri=uri,
            sql=sql,
            out_file=tmp_pq_file,
            modified=modified,
            row_group_size=row_group_size,
            tz=tz,
            adbc_batch_size_hint_bytes=adbc_batch_size_hint_bytes,
            adbc_use_copy=adbc_use_copy,
        )
        if result is None:
            print(f"No rows returned for {schema}.{alt_table_name}; no parquet file created.")
            return None

        if archive and pq_file.exists():
            pq_archive(file_name=pq_file, archive_dir=archive_dir)

        promote_temp_parquet(tmp_pq_file, pq_file)
        return str(pq_file)

    query = read_postgres_query(
        uri=uri,
        sql=sql,
        threads=threads,
    )
    wrote_rows = write_record_batch_reader_to_parquet(
        query.fetch_arrow_reader(),
        tmp_pq_file,
        modified=modified,
        row_group_size=row_group_size,
        tz=tz,
        total_rows=getattr(query, "total_rows", None),
        progress_label=f"{schema}.{alt_table_name}",
    )
    if not wrote_rows:
        print(f"No rows returned for {schema}.{alt_table_name}; no parquet file created.")
        return None

    if archive and pq_file.exists():
        pq_archive(file_name=pq_file, archive_dir=archive_dir)

    promote_temp_parquet(tmp_pq_file, pq_file)
    return str(pq_file)

def wrds_pg_to_pg(
    table_name,
    schema,
    *,
    wrds_id=None,
    dst_user=None,
    dst_host=None,
    dst_database=None,
    dst_port=None,
    dst_schema=None,
    col_types=None,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    tz="UTC",
    create_roles=True,
):
    """Write a WRDS PostgreSQL table to another PostgreSQL database.

    Parameters
    ----------
    table_name : str
        Name of the WRDS source table.

    schema : str
        Name of the WRDS source schema.

    wrds_id : str, optional
        WRDS user ID used to access the WRDS PostgreSQL service. If omitted,
        resolve from ``WRDS_ID`` / ``WRDS_USER`` and related `.env`
        configuration.

    dst_user, dst_host, dst_database, dst_port : optional
        Connection settings for the destination PostgreSQL database.

    dst_schema : str, optional
        Destination PostgreSQL schema. If omitted, defaults to ``schema``.

    col_types : dict, optional
        Explicit destination PostgreSQL column types for selected columns.

    obs : int, optional
        Number of rows to copy from WRDS. Implemented with SQL ``LIMIT``.

    alt_table_name : str, optional
        Destination PostgreSQL table name. If omitted, defaults to
        ``table_name``.

    keep, drop : str or iterable, optional
        Regex pattern(s) describing columns to keep or drop before loading
        the destination table. If both are supplied, ``drop`` is applied
        first.

    tz : str, optional
        Default timezone used when normalizing timestamp columns.

    create_roles : bool, optional
        If ``True``, ensure destination schema owner and access roles exist
        and apply ownership and grants to the loaded table.

    Returns
    -------
    bool
        ``True`` after the destination table has been created or replaced.

    Examples
    ----------
    >>> wrds_pg_to_pg("dsi", "crsp", dst_database="research")
    >>> wrds_pg_to_pg("company", "comp", dst_schema="comp_mirror", obs=1000)
    """
    from .postgres.wrds import resolve_wrds_id

    wrds_id = resolve_wrds_id(wrds_id)

    return db_to_pg(
        table_name,
        schema,
        user=wrds_id,
        host="wrds-pgdata.wharton.upenn.edu",
        database="wrds",
        port=9737,
        dst_user=dst_user,
        dst_host=dst_host,
        dst_database=dst_database,
        dst_port=dst_port,
        dst_schema=dst_schema,
        col_types=col_types,
        obs=obs,
        alt_table_name=alt_table_name,
        keep=keep,
        drop=drop,
        tz=tz,
        create_roles=create_roles,
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
    engine: str | None = None,
    numeric_mode: str | None = None,
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

    engine : {"duckdb", "adbc"}, optional
        Query execution engine used to read PostgreSQL data before writing
        Parquet.

    numeric_mode : {"text", "float64", "decimal"} or None, optional
        Handling for PostgreSQL ``NUMERIC`` columns. ``None`` keeps the
        engine-specific default: native decimals on DuckDB, text-backed
        numerics on ADBC.

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
    from .postgres.schema import db_schema_tables

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
                database=dbname,
                port=port,
                data_dir=data_dir,
                row_group_size=row_group_size,
                threads=threads,
                batched=batched,
                engine=engine,
                numeric_mode=numeric_mode,
                archive=archive,
                archive_dir=archive_dir,
            )
        )

    return results


def _update_pq(
    *,
    table_name,
    schema,
    source_kind,
    source_comment,
    update_callable,
    data_dir=None,
    force=False,
    alt_table_name=None,
    modified=_MODIFIED_DEFAULT,
    **update_kwargs,
):
    from .files.parquet import get_modified_pq
    from .files.paths import get_pq_file
    from .sync.modified import modified_info, update_available

    if not alt_table_name:
        alt_table_name = table_name

    pq_file = get_pq_file(table_name=alt_table_name, schema=schema, data_dir=data_dir)
    pq_comment = get_modified_pq(pq_file)
    src_mod = modified_info(source_kind, source_comment)
    pq_mod = modified_info("pq", pq_comment)

    if force:
        print("Forcing update based on user request.")
    elif src_mod.dt is None:
        print(
            f"Could not determine whether {schema}.{alt_table_name} needs an update "
            "because the source table has no parseable last-modified comment."
        )
        print("Set `force=True` to export the table anyway.")
        return
    elif not update_available(src=src_mod, dst=pq_mod):
        print(f"{schema}.{alt_table_name} already up to date.")
        return
    else:
        print(f"Updated {schema}.{alt_table_name} is available.")
        print(f"Beginning file download at {get_now()} UTC.")

    update_call = dict(
        table_name=table_name,
        schema=schema,
        data_dir=data_dir,
        alt_table_name=alt_table_name,
        **update_kwargs,
    )
    if modified is not _MODIFIED_DEFAULT:
        update_call["modified"] = modified

    pq_file = update_callable(**update_call)
    if pq_file is None:
        print(f"No file download completed at {get_now()} UTC (no rows returned).")
    else:
        print(f"Completed file download at {get_now()} UTC.")

    return pq_file


def pg_update_pq(
    table_name,
    schema,
    *,
    user=None,
    host=None,
    database=None,
    port=None,
    data_dir=None,
    force=False,
    col_types=None,
    row_group_size=1048576,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    where=None,
    batched=True,
    threads=3,
    tz="UTC",
    engine=None,
    numeric_mode=None,
    adbc_batch_size_hint_bytes=None,
    adbc_use_copy=None,
    archive=False,
    archive_dir=None,
):
    """Export a local PostgreSQL table to Parquet when the source is newer."""
    from .postgres.comments import get_pg_comment

    pg_comment = get_pg_comment(
        table_name=table_name,
        schema=schema,
        user=user,
        host=host,
        dbname=database,
        port=port,
    )

    return _update_pq(
        table_name=table_name,
        schema=schema,
        source_kind="pg",
        source_comment=pg_comment,
        update_callable=db_to_pq,
        data_dir=data_dir,
        force=force,
        alt_table_name=alt_table_name,
        user=user,
        host=host,
        database=database,
        port=port,
        col_types=col_types,
        row_group_size=row_group_size,
        obs=obs,
        keep=keep,
        drop=drop,
        where=where,
        batched=batched,
        threads=threads,
        tz=tz,
        engine=engine,
        numeric_mode=numeric_mode,
        adbc_batch_size_hint_bytes=adbc_batch_size_hint_bytes,
        adbc_use_copy=adbc_use_copy,
        archive=archive,
        archive_dir=archive_dir,
    )

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
    where=None,
    batched=True,
    threads=3,
    tz="UTC",
    engine=None,
    numeric_mode=None,
    adbc_batch_size_hint_bytes=None,
    adbc_use_copy=None,
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

    engine : {"duckdb", "adbc"} [Optional]
        Query execution engine used to read PostgreSQL data before writing
        Parquet.

    numeric_mode : {"text", "float64", "decimal"} or None [Optional]
        Handling for PostgreSQL ``NUMERIC`` columns. ``None`` keeps the
        engine-specific default: native decimals on DuckDB, text-backed
        numerics on ADBC. Explicit ``col_types`` entries take precedence.

    adbc_batch_size_hint_bytes : int [Optional]
        On the ADBC path, hint the PostgreSQL ADBC driver about the desired
        Arrow batch size in bytes.

    adbc_use_copy : bool [Optional]
        On the ADBC path, enable or disable the PostgreSQL driver's ``COPY``
        optimization explicitly.
    
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
    from .postgres.comments import get_wrds_comment
    from .postgres.comments import get_wrds_conn
    from .postgres.introspect import table_exists
    from .credentials import ensure_wrds_access

    wrds_id = ensure_wrds_access(wrds_id)
        
    if not sas_schema:
        sas_schema = schema

    if not alt_table_name:
        alt_table_name = table_name

    with get_wrds_conn(wrds_id) as conn:
        if not table_exists(conn, schema, table_name):
            print(f"Table with name {table_name} does not exist.")
            return None
                
    wrds_comment = get_wrds_comment(
        table_name=table_name,
        schema=schema,
        wrds_id=wrds_id,
        use_sas=use_sas,
        sas_schema=sas_schema,
        encoding=encoding,
    )
           
    wrds_kind = "wrds_sas" if use_sas else "wrds_pg"
    update_kwargs = dict(
        table_name=table_name,
        schema=schema,
        source_kind=wrds_kind,
        source_comment=wrds_comment,
        update_callable=wrds_pg_to_pq,
        data_dir=data_dir,
        force=force,
        alt_table_name=alt_table_name,
        wrds_id=wrds_id,
        col_types=col_types,
        row_group_size=row_group_size,
        obs=obs,
        keep=keep,
        drop=drop,
        where=where,
        batched=batched,
        threads=threads,
        tz=tz,
        engine=engine,
        numeric_mode=numeric_mode,
        adbc_batch_size_hint_bytes=adbc_batch_size_hint_bytes,
        adbc_use_copy=adbc_use_copy,
        archive=archive,
        archive_dir=archive_dir,
    )
    if use_sas:
        update_kwargs["modified"] = wrds_comment

    return _update_pq(**update_kwargs)
    
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
    from .files.paths import pq_list_files

    pq_files = pq_list_files(schema=schema, data_dir=data_dir)

    for pq_file in pq_files:
        wrds_update_pq(
            table_name=pq_file,
            schema=schema,
            data_dir=data_dir,
            threads=threads,
            archive=archive,
        )

    return pq_files                      
