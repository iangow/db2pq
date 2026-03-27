def _backend_to_uri(backend):
    from urllib.parse import quote_plus

    info = backend.con.info
    auth = ""
    user = getattr(info, "user", None)
    password = getattr(info, "password", None)
    if user:
        auth = quote_plus(str(user))
        if password is not None:
            auth = f"{auth}:{quote_plus(str(password))}"
        auth = f"{auth}@"

    return f"postgresql://{auth}{info.host}:{info.port}/{info.dbname}"


def ibis_to_pq(
    table,
    out_file,
    *,
    engine=None,
    row_group_size=1024 * 1024,
    threads=None,
    tz="UTC",
    adbc_batch_size_hint_bytes=None,
    adbc_use_copy=None,
    **writer_kwargs,
):
    """Write an Ibis PostgreSQL table expression to a parquet file.

    This helper compiles an Ibis PostgreSQL expression to SQL and runs it
    through the same PostgreSQL export engines used elsewhere in ``db2pq``.
    The resulting Arrow stream is written directly to the destination
    Parquet file.

    ``ibis_to_pq()`` currently supports Ibis expressions backed by a
    PostgreSQL connection. To use it, install the optional dependency:

    ``pip install "db2pq[ibis]"``

    Parameters
    ----------
    table :
        Ibis table expression backed by PostgreSQL. This may be a base
        table or a derived expression such as a filtered, selected, or
        mutated query.

    out_file : str or path-like
        Destination parquet file path.

    engine : {"duckdb", "adbc"} [Optional]
        Query execution engine used to run the compiled PostgreSQL SQL.
        If omitted, uses the configured default engine from
        ``set_default_engine()`` / ``DB2PQ_ENGINE``.

    row_group_size : int [Optional]
        Maximum number of rows in each written Parquet row group.

    threads : int [Optional]
        Maximum DuckDB worker threads to use when ``engine="duckdb"``.

    tz : str [Optional]
        Time zone assumption for naive PostgreSQL timestamps before
        normalizing Parquet output to UTC.

    adbc_batch_size_hint_bytes : int [Optional]
        ADBC batch size hint in bytes when ``engine="adbc"``.

    adbc_use_copy : bool [Optional]
        Explicitly enable or disable the PostgreSQL ADBC driver's ``COPY``
        optimization when ``engine="adbc"``.

    **writer_kwargs
        Additional keyword arguments passed to ``pyarrow.parquet.ParquetWriter``.
        This can be used to set options such as ``compression="zstd"``.

    Returns
    -------
    pq_file : str
        Name of parquet file created.

    Raises
    ------
    TypeError
        If the supplied Ibis expression is not backed by PostgreSQL, or
        if PostgreSQL connection information cannot be determined from
        the backend.

    Examples
    --------
    >>> from db2pq import ibis_to_pq
    >>> expr = con.table("my_table").filter(lambda t: t.id > 100)
    >>> ibis_to_pq(expr, "my_table.parquet")
    'my_table.parquet'

    >>> expr = con.table("my_table").select("id", "value")
    >>> ibis_to_pq(expr, "my_table.parquet", compression="zstd")
    'my_table.parquet'
    """
    from .config import get_default_engine
    from .files.parquet import write_record_batch_reader_to_parquet
    from .postgres.adbc import export_postgres_query_via_adbc
    from .postgres.duckdb_pg import read_postgres_query

    backend = table.get_backend()
    backend_name = getattr(backend, "name", None)
    if backend_name != "postgres":
        raise TypeError("ibis_to_pq() currently requires a PostgreSQL-backed Ibis table")

    if not hasattr(backend, "con") or not hasattr(backend.con, "info"):
        raise TypeError("Could not determine PostgreSQL connection info from the Ibis backend")

    uri = _backend_to_uri(backend)
    sql = str(table.compile())
    if engine is None:
        engine = get_default_engine()
    engine = engine.lower()

    if engine == "adbc":
        return export_postgres_query_via_adbc(
            uri=uri,
            sql=sql,
            out_file=out_file,
            row_group_size=row_group_size,
            tz=tz,
            adbc_batch_size_hint_bytes=adbc_batch_size_hint_bytes,
            adbc_use_copy=adbc_use_copy,
            parquet_writer_kwargs=writer_kwargs,
        )

    if engine != "duckdb":
        raise ValueError("engine must be either 'duckdb' or 'adbc'")

    query = read_postgres_query(
        uri=uri,
        sql=sql,
        threads=threads,
    )
    wrote_rows = write_record_batch_reader_to_parquet(
        query.fetch_arrow_reader(),
        out_file,
        row_group_size=row_group_size,
        tz=tz,
        parquet_writer_kwargs=writer_kwargs,
    )
    return str(out_file) if wrote_rows else None
