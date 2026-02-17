from .introspect import get_table_columns, get_table_column_types
from .select_sql import build_wrds_select_sql, select_columns
from .duckdb_ddl import create_table_from_select_duckdb
from .copy import copy_wrds_select_to_pg_table
from .comments import get_pg_comment_conn, get_pg_conn, get_wrds_conn, set_table_comment
from .wrds import get_wrds_uri
from ._defaults import resolve_uri
from ..core import get_now
# from ..files.paths import resolve_data_dir  # later, when you add pq piece
from ..sync.modified import modified_info, update_available

def _schema_exists(conn, schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s LIMIT 1", (schema,))
        return cur.fetchone() is not None

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
    - ``tz`` (default ``"UTC"``) is used to convert source
      ``timestamp without time zone`` columns via ``AT TIME ZONE``.

    Returns
    -------
    bool
        ``True`` if an update was performed, ``False`` if the destination
        table was already up to date.
    """
    
    uri = resolve_uri(user=user, host=host, dbname=dbname, port=port)

    alt_table_name = alt_table_name or table_name
    source_schema = wrds_schema or schema
    
    col_types = col_types or {}
    with get_wrds_conn(wrds_id) as wrds, get_pg_conn(uri) as pg:
        all_cols = get_table_columns(wrds, source_schema, table_name)
        source_col_types = get_table_column_types(wrds, source_schema, table_name)
        cols = select_columns(all_cols, keep=keep, drop=drop)
        if tz:
            selected_types = [source_col_types.get(c, "").strip().lower() for c in cols]
            n_naive_ts = sum(t == "timestamp without time zone" for t in selected_types)
            n_tz_ts = sum(t == "timestamp with time zone" for t in selected_types)
            if n_naive_ts > 0:
                print(
                    f"Applying tz='{tz}' conversion to "
                    f"{n_naive_ts} timestamp without time zone column(s)."
                )
            if n_tz_ts > 0:
                print(
                    f"No tz conversion applied to {n_tz_ts} "
                    "timestamp with time zone column(s)."
                )
        wrds_comment = get_pg_comment_conn(wrds, schema=source_schema,
                                                table_name=table_name)
        if not force:
            pg_comment = get_pg_comment_conn(pg, schema=schema,
                                             table_name=alt_table_name)
            wrds_mod = modified_info("wrds_pg", wrds_comment)
            pg_mod   = modified_info("pg", pg_comment)

            if not update_available(src=wrds_mod, dst=pg_mod):
                # optionally print why
                print(f"{schema}.{alt_table_name} already up to date.")
                return False
            print(f"Updated {schema}.{alt_table_name} is available.")
        else:
            print("Forcing update based on user request.")
        print(f"Beginning file import at {get_now()} UTC.")
        print(f"Importing data into {schema}.{alt_table_name}.")

        _ensure_schema_and_roles(pg, schema, create_roles=create_roles)
        
        duckdb_sql = build_wrds_select_sql(
            conn=pg,
            schema=source_schema,
            table=table_name,
            columns=cols,
            col_types=col_types,
            source_col_types=source_col_types,
            tz=tz,
            obs=obs,
            qualify="wrds",
        )
        
        copy_sql = build_wrds_select_sql(
            conn=pg,
            schema=source_schema,
            table=table_name,
            columns=cols,
            col_types=col_types,
            source_col_types=source_col_types,
            tz=tz,
            obs=obs,
            qualify=None,
        )

        # DuckDB uses conn strings, not the already-open psycopg conns
        create_table_from_select_duckdb(
            select_sql=duckdb_sql,
            wrds_uri=get_wrds_uri(wrds_id),
            dst_uri=uri,
            dst_schema=schema,
            dst_table=alt_table_name,
            drop_if_exists=True,
        )
        
        copy_wrds_select_to_pg_table(
            wrds_conn=wrds,
            pg_conn=pg,
            select_sql=copy_sql,
            dst_schema=schema,
            dst_table=alt_table_name,
            cols=cols,
            uri=uri
        )
    
        set_table_comment(pg, schema=schema, table_name=alt_table_name,
                          comment=wrds_comment)

        if create_roles:
            _apply_table_roles(pg, schema, alt_table_name)
        print(f"Completed file import at {get_now()} UTC.\n")
        return True
