# db2pq/postgres/schema.py
from __future__ import annotations

from ..credentials import ensure_pg_access
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
    """Get a list of relations in a PostgreSQL schema.

    Parameters
    ----------
    schema : str
        Name of the PostgreSQL schema to inspect.

    views : bool, optional
        If ``True``, include views in addition to base tables.

    user : str
        PostgreSQL user role.
    host : str
        PostgreSQL host name.
    database : str
        PostgreSQL database name.
    dbname : str
        Alias for ``database``.
    port : int
        PostgreSQL port.

    Returns
    -------
    list[str]
        Sorted relation names in the requested schema.

    Examples
    ----------
    >>> db_schema_tables("public")
    >>> db_schema_tables("crsp", views=True, database="research")
    """
    user, host, dbname, port = resolve_pg_connection(
        user=user,
        host=host,
        dbname=dbname or database,
        port=port,
    )
    ensure_pg_access(user=user, host=host, dbname=dbname, port=str(port))

    uri = f"postgresql://{user}@{host}:{port}/{dbname}"
    with get_pg_conn(uri) as conn:
        return _list_relations(conn, schema, views=views)


def wrds_get_tables(
    schema: str, *, wrds_id: str | None = None, views: bool = False
) -> list[str]:
    """Get a list of relations in a WRDS schema.

    Parameters
    ----------
    schema : str
        Name of the WRDS schema to inspect.

    wrds_id : str, optional
        WRDS user ID used to access the WRDS PostgreSQL service. If omitted,
        resolve from ``WRDS_ID`` / ``WRDS_USER`` and related `.env`
        configuration.

    views : bool, optional
        If ``True``, include views in addition to base tables.

    Returns
    -------
    list[str]
        Sorted relation names in the requested WRDS schema.

    Examples
    ----------
    >>> wrds_get_tables("crsp")
    >>> wrds_get_tables("comp", views=True)
    """
    with get_wrds_conn(wrds_id) as conn:
        return _list_relations(conn, schema, views=views)
