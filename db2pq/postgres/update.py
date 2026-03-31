from pathlib import Path
from time import gmtime, strftime

from .introspect import get_table_columns, get_table_column_types, table_exists
from .select_sql import plan_wrds_query
from .duckdb_ddl import create_table_from_select_duckdb
from .copy import copy_wrds_select_to_pg_table
from .comments import (
    get_pg_comment_conn,
    get_pg_conn,
    get_wrds_comment,
    get_wrds_conn,
    set_table_comment,
)
from .wrds import get_wrds_uri
from ._defaults import resolve_uri
from ..types import normalize_col_types
# from ..files.paths import resolve_data_dir  # later, when you add pq piece
from ..sync.modified import modified_info, update_available


def get_now():
    return strftime("%Y-%m-%d %H:%M:%S", gmtime())


def process_sql(
    sql,
    *,
    user=None,
    host=None,
    dbname=None,
    port=None,
    params=None,
):
    """
    Execute SQL against the destination PostgreSQL database used by
    ``wrds_update_pg()`` by default.

    Parameters
    ----------
    sql : str
        SQL statement to execute.
    user, host, dbname, port : optional
        PostgreSQL connection settings. If omitted, resolve from the same
        environment/default chain used by ``wrds_update_pg()``.
    params : sequence or mapping, optional
        Parameters passed to ``cursor.execute()``.

    Returns
    -------
    str
        Psycopg ``statusmessage`` for the executed statement.
    """
    uri = resolve_uri(user=user, host=host, dbname=dbname, port=port)

    with get_pg_conn(uri) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        status = cur.statusmessage
        conn.commit()

    return status

def _schema_exists(conn, schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s LIMIT 1", (schema,))
        return cur.fetchone() is not None

def _table_exists(conn, schema: str, table_name: str) -> bool:
    return table_exists(conn, schema, table_name)

def _role_exists(conn, role: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s LIMIT 1", (role,))
        return cur.fetchone() is not None

def _execute_ident_sql(conn, sql_template: str, *idents: str) -> None:
    from psycopg import sql as psql
    stmt = psql.SQL(sql_template).format(*(psql.Identifier(i) for i in idents))
    with conn.cursor() as cur:
        cur.execute(stmt)

def _create_role(conn, role: str) -> None:
    _execute_ident_sql(conn, "CREATE ROLE {}", role)

def _ensure_schema_and_roles(conn, schema: str, *, create_roles: bool) -> None:
    changed = False

    if not _schema_exists(conn, schema):
        _execute_ident_sql(conn, "CREATE SCHEMA {}", schema)
        changed = True

    if not create_roles:
        if changed:
            # DDL must be committed so separate connections (e.g., DuckDB DDL
            # using dst_uri) can resolve the new schema immediately.
            conn.commit()
        return

    access_role = f"{schema}_access"

    if not _role_exists(conn, schema):
        _create_role(conn, schema)
        changed = True
    if not _role_exists(conn, access_role):
        _create_role(conn, access_role)
        changed = True

    _execute_ident_sql(conn, "ALTER SCHEMA {} OWNER TO {}", schema, schema)
    _execute_ident_sql(conn, "GRANT USAGE ON SCHEMA {} TO {}", schema, access_role)
    changed = True

    if changed:
        # DDL must be committed so separate connections (e.g., DuckDB DDL
        # using dst_uri) can resolve the new schema immediately.
        conn.commit()

def _apply_table_roles(conn, schema: str, table_name: str) -> None:
    access_role = f"{schema}_access"
    _execute_ident_sql(conn, "ALTER TABLE {}.{} OWNER TO {}", schema, table_name, schema)
    _execute_ident_sql(conn, "GRANT SELECT ON {}.{} TO {}", schema, table_name, access_role)


def _duckdb_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _parquet_reader(pq_file: Path):
    import pyarrow.dataset as ds

    return ds.dataset(pq_file, format="parquet").scanner().to_reader()


def _duckdb_load_parquet_to_pg(*, pq_file: Path, dst_uri: str, dst_schema: str, dst_table_name: str) -> None:
    import duckdb

    con = duckdb.connect()
    try:
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.execute(
            f"ATTACH '{dst_uri}' AS pg (TYPE postgres, SCHEMA '{dst_schema}')"
        )
        con.execute(
            f"DROP TABLE IF EXISTS pg.{_duckdb_ident(dst_schema)}.{_duckdb_ident(dst_table_name)}"
        )
        # Materialize schema first, then load rows. This is faster than CTAS
        # from read_parquet() on the local benchmark path we tested.
        con.execute(
            (
                f"CREATE TABLE pg.{_duckdb_ident(dst_schema)}.{_duckdb_ident(dst_table_name)} "
                "AS SELECT * FROM read_parquet(?) LIMIT 0"
            ),
            [str(pq_file)],
        )
        con.execute(
            (
                f"INSERT INTO pg.{_duckdb_ident(dst_schema)}.{_duckdb_ident(dst_table_name)} "
                "SELECT * FROM read_parquet(?)"
            ),
            [str(pq_file)],
        )
    finally:
        con.close()


def parquet_write_pg(
    *,
    pq_file,
    dst_uri: str,
    dst_schema: str,
    dst_table_name: str,
    engine: str = "duckdb",
    create_roles: bool = True,
    source_comment: str | None = None,
):
    """
    Write a parquet file into PostgreSQL, replacing the destination table.
    """
    pq_file = Path(pq_file).expanduser()
    if not pq_file.exists():
        raise FileNotFoundError(f"Parquet file not found: {pq_file}")

    engine = engine.lower()
    if engine not in {"duckdb", "adbc"}:
        raise ValueError("engine must be either 'duckdb' or 'adbc'")

    with get_pg_conn(dst_uri) as pg_conn:
        print(f"Beginning file import at {get_now()} UTC.")
        print(f"Importing data into {dst_schema}.{dst_table_name}.")

        _ensure_schema_and_roles(pg_conn, dst_schema, create_roles=create_roles)

        if engine == "duckdb":
            _duckdb_load_parquet_to_pg(
                pq_file=pq_file,
                dst_uri=dst_uri,
                dst_schema=dst_schema,
                dst_table_name=dst_table_name,
            )
        else:
            import adbc_driver_postgresql.dbapi as adbc_dbapi

            reader = _parquet_reader(pq_file)
            with adbc_dbapi.connect(dst_uri) as adbc_conn, adbc_conn.cursor() as cur:
                cur.adbc_ingest(
                    dst_table_name,
                    reader,
                    mode="replace",
                    db_schema_name=dst_schema,
                )
                adbc_conn.commit()

        set_table_comment(
            pg_conn,
            schema=dst_schema,
            table_name=dst_table_name,
            comment=source_comment,
        )

        if create_roles:
            _apply_table_roles(pg_conn, dst_schema, dst_table_name)

        print(f"Completed file import at {get_now()} UTC.\n")
        return True

def _write_pg_table_from_source(
    *,
    source_conn,
    source_uri: str,
    source_schema: str,
    source_table_name: str,
    pg_conn,
    dst_uri: str,
    dst_schema: str,
    dst_table_name: str,
    col_types=None,
    obs=None,
    keep=None,
    drop=None,
    create_roles=True,
    source_comment=None,
    tz="UTC",
):
    col_types = normalize_col_types(col_types, engine="postgres") or {}

    all_cols = get_table_columns(source_conn, source_schema, source_table_name)
    source_col_types = get_table_column_types(source_conn, source_schema, source_table_name)
    plan = plan_wrds_query(
        conn=pg_conn,
        schema=source_schema,
        table=source_table_name,
        all_cols=all_cols,
        source_col_types=source_col_types,
        col_types=col_types,
        keep=keep,
        drop=drop,
        tz=tz,
        obs=obs,
        qualified_alias="wrds",
    )
    if tz:
        if plan.n_naive_ts > 0:
            print(
                f"Applying tz='{tz}' conversion to "
                f"{plan.n_naive_ts} timestamp without time zone column(s)."
            )
        if plan.n_tz_ts > 0:
            print(
                f"No tz conversion applied to {plan.n_tz_ts} "
                "timestamp with time zone column(s)."
            )

    if source_comment is None:
        source_comment = get_pg_comment_conn(
            source_conn,
            schema=source_schema,
            table_name=source_table_name,
        )
    print(f"Beginning file import at {get_now()} UTC.")
    print(f"Importing data into {dst_schema}.{dst_table_name}.")

    _ensure_schema_and_roles(pg_conn, dst_schema, create_roles=create_roles)

    create_table_from_select_duckdb(
        select_sql=plan.qualified_sql,
        wrds_uri=source_uri,
        dst_uri=dst_uri,
        dst_schema=dst_schema,
        dst_table=dst_table_name,
        drop_if_exists=True,
    )

    copy_wrds_select_to_pg_table(
        wrds_conn=source_conn,
        pg_conn=pg_conn,
        select_sql=plan.sql,
        dst_schema=dst_schema,
        dst_table=dst_table_name,
        cols=plan.columns,
        uri=dst_uri
    )

    set_table_comment(
        pg_conn,
        schema=dst_schema,
        table_name=dst_table_name,
        comment=source_comment,
    )

    if create_roles:
        _apply_table_roles(pg_conn, dst_schema, dst_table_name)
    print(f"Completed file import at {get_now()} UTC.\n")
    return True

def postgres_write_pg(
    table_name,
    schema,
    *,
    src_uri,
    dst_uri,
    dst_schema=None,
    col_types=None,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    create_roles=True,
    source_comment=None,
    tz="UTC",
):
    """
    Write a PostgreSQL table into another PostgreSQL database.

    This is a straight PG-to-PG copy primitive with no update-date checks.
    """
    dst_schema = dst_schema or schema
    alt_table_name = alt_table_name or table_name

    with get_pg_conn(src_uri) as src, get_pg_conn(dst_uri) as dst:
        return _write_pg_table_from_source(
            source_conn=src,
            source_uri=src_uri,
            source_schema=schema,
            source_table_name=table_name,
            pg_conn=dst,
            dst_uri=dst_uri,
            dst_schema=dst_schema,
            dst_table_name=alt_table_name,
            col_types=col_types,
            obs=obs,
            keep=keep,
            drop=drop,
            create_roles=create_roles,
            source_comment=source_comment,
            tz=tz,
        )

def wrds_update_pg(
    table_name,
    schema,
    *,
    wrds_id=None,
    col_types=None,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    user=None,
    host=None,
    dbname=None,
    port=None,
    force=False,
    create_roles=True,
    wrds_schema=None,
    use_sas=False,
    encoding="utf-8",
    tz="UTC",
):
    """
    Materialize a WRDS PostgreSQL table into a local PostgreSQL database.

    The destination table is created from scratch based on the SQL SELECT
    used to extract the data (via DuckDB schema inference), then populated
    using PostgreSQL binary COPY for performance.

    Notes
    -----
    - Any existing destination table is dropped.
    - `keep`/`drop` use regex matching on source column names.
    - If both `drop` and `keep` are provided, `drop` is applied first.
    - Update / fingerprint logic is handled elsewhere (or added later).
    - If ``create_roles`` is True, ensures schema owner role (``<schema>``)
      and read-only role (``<schema>_access``) exist, then applies grants.
    - If ``wrds_schema`` is provided, it is used as the source WRDS schema while
      data are still written to destination ``schema``.
    - If ``use_sas`` is True, freshness and destination comment metadata are
      derived from SAS metadata instead of WRDS PostgreSQL comments.
    - ``tz`` (default ``"UTC"``) is used to convert source
      ``timestamp without time zone`` columns via ``AT TIME ZONE``.

    Returns
    -------
    bool
        ``True`` if an update was performed, ``False`` if the destination
        table was already up to date.
    """
    from ..credentials import ensure_wrds_access

    uri = resolve_uri(user=user, host=host, dbname=dbname, port=port)
    wrds_id = ensure_wrds_access(wrds_id)
    alt_table_name = alt_table_name or table_name
    source_schema = wrds_schema or schema
    source_uri = get_wrds_uri(wrds_id)

    with get_wrds_conn(wrds_id) as wrds, get_pg_conn(uri) as pg:
        if not _table_exists(wrds, source_schema, table_name):
            print(f"Table with name {table_name} does not exist.")
            return False

        wrds_comment = get_wrds_comment(
            table_name=table_name,
            schema=source_schema,
            wrds_id=wrds_id,
            use_sas=use_sas,
            sas_schema=source_schema,
            encoding=encoding,
        )
        if not force:
            if not _table_exists(pg, schema, alt_table_name):
                print(f"{schema}.{alt_table_name} does not exist in destination.")
                print("Getting from WRDS.")
            else:
                pg_comment = get_pg_comment_conn(pg, schema=schema, table_name=alt_table_name)
                wrds_kind = "wrds_sas" if use_sas else "wrds_pg"
                wrds_mod = modified_info(wrds_kind, wrds_comment)
                pg_mod = modified_info("pg", pg_comment)

                if not update_available(src=wrds_mod, dst=pg_mod):
                    print(f"{schema}.{alt_table_name} already up to date.")
                    return False
                print(f"Updated {schema}.{alt_table_name} is available.")
        else:
            print("Forcing update based on user request.")

    return postgres_write_pg(
        table_name=table_name,
        schema=source_schema,
        src_uri=source_uri,
        dst_uri=uri,
        dst_schema=schema,
        col_types=col_types,
        obs=obs,
        alt_table_name=alt_table_name,
        keep=keep,
        drop=drop,
        create_roles=create_roles,
        source_comment=wrds_comment,
        tz=tz,
    )


def pq_to_pg(
    table_name,
    schema,
    *,
    data_dir=None,
    user=None,
    host=None,
    dbname=None,
    database=None,
    port=None,
    dst_schema=None,
    alt_table_name=None,
    engine="duckdb",
    create_roles=True,
    source_comment=None,
):
    """
    Write a parquet file from the local repository into PostgreSQL.
    """
    from ..files.parquet import get_modified_pq
    from ..files.paths import get_pq_file

    uri = resolve_uri(user=user, host=host, dbname=dbname or database, port=port)
    pq_file = get_pq_file(table_name=table_name, schema=schema, data_dir=data_dir)
    dst_schema = dst_schema or schema
    alt_table_name = alt_table_name or table_name
    source_comment = get_modified_pq(pq_file) if source_comment is None else source_comment

    return parquet_write_pg(
        pq_file=pq_file,
        dst_uri=uri,
        dst_schema=dst_schema,
        dst_table_name=alt_table_name,
        engine=engine,
        create_roles=create_roles,
        source_comment=source_comment,
    )


def pq_update_pg(
    table_name,
    schema,
    *,
    data_dir=None,
    user=None,
    host=None,
    dbname=None,
    database=None,
    port=None,
    dst_schema=None,
    alt_table_name=None,
    engine="duckdb",
    force=False,
    create_roles=True,
):
    """
    Materialize a parquet file into PostgreSQL when the parquet source is newer.
    """
    from ..files.parquet import get_modified_pq
    from ..files.paths import get_pq_file

    uri = resolve_uri(user=user, host=host, dbname=dbname or database, port=port)
    pq_file = get_pq_file(table_name=table_name, schema=schema, data_dir=data_dir)
    dst_schema = dst_schema or schema
    alt_table_name = alt_table_name or table_name

    if not pq_file.exists():
        raise FileNotFoundError(f"Parquet file not found: {pq_file}")

    pq_comment = get_modified_pq(pq_file)
    pq_mod = modified_info("pq", pq_comment)

    with get_pg_conn(uri) as pg:
        if force:
            print("Forcing update based on user request.")
        elif pq_mod.dt is None:
            print(
                f"Could not determine whether {dst_schema}.{alt_table_name} needs an update "
                "because the source parquet file has no parseable last_modified metadata."
            )
            print("Set `force=True` to import the parquet file anyway.")
            return False
        elif not _table_exists(pg, dst_schema, alt_table_name):
            print(f"{dst_schema}.{alt_table_name} does not exist in destination.")
            print("Importing from parquet.")
        else:
            pg_comment = get_pg_comment_conn(pg, schema=dst_schema, table_name=alt_table_name)
            pg_mod = modified_info("pg", pg_comment)
            if not update_available(src=pq_mod, dst=pg_mod):
                print(f"{dst_schema}.{alt_table_name} already up to date.")
                return False
            print(f"Updated {dst_schema}.{alt_table_name} is available.")

    return pq_to_pg(
        table_name=table_name,
        schema=schema,
        data_dir=data_dir,
        user=user,
        host=host,
        dbname=dbname,
        database=database,
        port=port,
        dst_schema=dst_schema,
        alt_table_name=alt_table_name,
        engine=engine,
        create_roles=create_roles,
        source_comment=pq_comment,
    )
