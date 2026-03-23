def _backend_to_uri(backend):
    from urllib.parse import quote_plus

    info = backend.con.info
    return (
        f"postgresql://{quote_plus(info.user)}:{quote_plus(info.password)}"
        f"@{info.host}:{info.port}/{info.dbname}"
    )


def ibis_to_pq(table, out_file, **writer_kwargs):
    """Write an Ibis PostgreSQL table expression to a parquet file.

    This helper executes the compiled SQL for an Ibis table expression
    through the ADBC PostgreSQL driver and streams the result directly
    into ``pyarrow.parquet.ParquetWriter``.

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

    **writer_kwargs
        Additional keyword arguments passed to ``pyarrow.parquet.ParquetWriter``.
        This can be used to set options such as ``compression="zstd"``.

    Returns
    -------
    pq_file : str
        Name of parquet file created.

    Raises
    ------
    ImportError
        If ``adbc-driver-postgresql`` is not installed.

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
    try:
        import adbc_driver_postgresql.dbapi
    except ImportError as exc:
        raise ImportError(
            "ibis_to_pq() requires adbc-driver-postgresql. "
            'Install it with: pip install "db2pq[ibis]"'
        ) from exc

    import pyarrow.parquet as pq

    backend = table.get_backend()
    backend_name = getattr(backend, "name", None)
    if backend_name != "postgres":
        raise TypeError("ibis_to_pq() currently requires a PostgreSQL-backed Ibis table")

    if not hasattr(backend, "con") or not hasattr(backend.con, "info"):
        raise TypeError("Could not determine PostgreSQL connection info from the Ibis backend")

    uri = _backend_to_uri(backend)
    sql = str(table.compile())

    with adbc_driver_postgresql.dbapi.connect(uri) as adbc_conn:
        with adbc_conn.cursor() as cur:
            cur.execute(sql)
            reader = cur.fetch_record_batch()

            with pq.ParquetWriter(out_file, reader.schema, **writer_kwargs) as writer:
                for batch in reader:
                    writer.write_batch(batch)

    return str(out_file)
