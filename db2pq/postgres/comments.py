from __future__ import annotations

import psycopg

from ._defaults import resolve_pg_connection
from .wrds import resolve_wrds_id, get_wrds_uri

def get_pg_comment_conn(conn, *, schema: str, table_name: str) -> str | None:
    sql = """
    SELECT obj_description(
             to_regclass(%s),
             'pg_class'
           ) AS comment
    """
    fqname = f"{schema}.{table_name}"

    with conn.cursor() as cur:
        cur.execute(sql, (fqname,))
        row = cur.fetchone()

    return row[0] if row else None

def get_table_comment(conn, *,  schema: str, table_name: str) -> str:
    """Return the table comment from pg_class, or '' if none exists."""

    sql = text(
        """
        SELECT obj_description(
            to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)),
            'pg_class'
        )
        """
    )

    return conn.execute(sql, {"schema": schema, "table": table_name}).scalar() or ""

from psycopg import sql as psql

def set_table_comment(conn, *, schema: str, table_name: str, comment: str | None) -> None:
    """
    Set (or clear) a PostgreSQL table comment using psycopg.
    """
    stmt = psql.SQL("COMMENT ON TABLE {}.{} IS {}").format(
        psql.Identifier(schema),
        psql.Identifier(table_name),
        psql.Literal(comment),
    )

    with conn.cursor() as cur:
        cur.execute(stmt)

def get_pg_comment(
    table_name: str,
    schema: str,
    *,
    user: str | None = None,
    host: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
) -> str | None:
    user, host, dbname, port = resolve_pg_connection(
        user=user, host=host, dbname=dbname, port=port
    )

    conninfo = f"postgresql://{user}@{host}:{port}/{dbname}"
    with psycopg.connect(conninfo) as conn:
        return get_pg_comment_conn(conn, schema=schema, table_name=table_name)

def get_pg_conn(uri):
    return psycopg.connect(uri)

def get_wrds_conn(wrds_id: str | None = None):
    wrds_id = resolve_wrds_id(wrds_id)
    return get_pg_conn(get_wrds_uri(wrds_id))

def get_wrds_comment(
    table_name: str,
    schema: str,
    *,
    wrds_id: str | None = None,
) -> str | None:
    with get_wrds_conn(wrds_id) as conn:
        return get_pg_comment_conn(
            conn,
            schema=schema,
            table_name=table_name,
        )
