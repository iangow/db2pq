# db2pq/postgres/schema.py
from __future__ import annotations

import ibis

from ._defaults import resolve_pg_connection


def db_schema_tables(
    schema: str,
    *,
    user: str | None = None,
    host: str | None = None,
    database: str | None = None,
    port: int | None = None,
) -> list[str]:
    """Get list of all tables in a PostgreSQL schema."""
    user, host, database, port = resolve_pg_connection(
        user=user, host=host, database=database, port=port
    )

    con = ibis.postgres.connect(user=user, host=host, port=port, database=database)
    return con.list_tables(database=schema)