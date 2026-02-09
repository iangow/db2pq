from .introspect import get_table_columns
from .select_sql import build_wrds_select_sql, select_columns
from .duckdb_ddl import create_table_from_select_duckdb
from .copy import copy_wrds_select_to_pg_table
from .comments import get_pg_comment_conn, get_pg_conn, get_wrds_conn
from ._defaults import get_wrds_url, resolve_uri
# from ..files.paths import resolve_data_dir  # later, when you add pq piece

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
    temp_suffix=None,
):
    """
    Materialize a WRDS PostgreSQL table into a local PostgreSQL database.

    The destination table is created from scratch based on the SQL SELECT
    used to extract the data (via DuckDB schema inference), then populated
    using PostgreSQL binary COPY for performance.

    Notes
    -----
    - Any existing destination table is dropped.
    - Update / fingerprint logic is handled elsewhere (or added later).
    """
    
    uri = resolve_uri(user=user, host=host, dbname=dbname, port=port)
    print(f"uri: {uri}")

    alt_table_name = alt_table_name or table_name
    temp_suffix = "_temp" if temp_suffix is None else temp_suffix
    temp_name = f"{alt_table_name}{temp_suffix}"
    
    col_types = col_types or {}

    with get_wrds_conn(wrds_id) as wrds, get_pg_conn(uri) as pg:
        all_cols = get_table_columns(wrds, schema, table_name)
        cols = select_columns(all_cols, keep=keep, drop=drop)
        
        comment = get_pg_comment_conn(wrds, schema=schema,
                                      table_name=table_name)
        print(f"WRDS comment: {comment}")
        
        duckdb_sql = build_wrds_select_sql(
            conn=pg,
            schema=schema,
            table=table_name,
            columns=cols,
            col_types=col_types,
            obs=obs,
            qualify="wrds",
        )
        
        copy_sql = build_wrds_select_sql(
            conn=pg,
            schema=schema,
            table=table_name,
            columns=cols,
            col_types=col_types,
            obs=obs,
            qualify=None,
        )

        # DuckDB uses conn strings, not the already-open psycopg conns
        create_table_from_select_duckdb(
            select_sql=duckdb_sql,
            wrds_uri=get_wrds_url(wrds_id),
            dst_uri=uri,
            dst_schema=schema,
            dst_table=temp_name,
            drop_if_exists=True,
        )
        
        copy_wrds_select_to_pg_table(
            wrds_conn=wrds,
            pg_conn=pg,
            select_sql=copy_sql,
            dst_schema=schema,
            dst_table=temp_name,
            cols=cols,
            uri=uri
        )

    return f"{schema}.{temp_name}"
