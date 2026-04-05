# db2pq/postgres/schema.py
from __future__ import annotations

from ._defaults import resolve_pg_connection
from .comments import get_pg_conn, get_wrds_conn


def _list_relations(conn, schema: str, *, views: bool = False) -> list[str]:
    with conn.cursor() as cur:
        if views:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
                """,
                (schema,),
            )
        else:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
        return [row[0] for row in cur.fetchall()]

def db_schema_tables(
    schema: str,
    *,
    views: bool = False,
    user: str | None = None,
    host: str | None = None,
    database: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
) -> list[str]:
    """Get list of all tables in a PostgreSQL schema."""
    user, host, dbname, port = resolve_pg_connection(
        user=user,
        host=host,
        dbname=dbname or database,
        port=port,
    )

    uri = f"postgresql://{user}@{host}:{port}/{dbname}"
    with get_pg_conn(uri) as conn:
        return _list_relations(conn, schema, views=views)


def wrds_get_tables(
    schema: str, *, wrds_id: str | None = None, views: bool = False
) -> list[str]:
    """Get list of WRDS tables in a schema, optionally including views."""
    with get_wrds_conn(wrds_id) as conn:
        return _list_relations(conn, schema, views=views)
