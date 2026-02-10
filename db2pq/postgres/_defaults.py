from __future__ import annotations

import os
import getpass

def resolve_pg_connection(
    *,
    user: str | None = None,
    host: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
) -> tuple[str, str, str, int]:
    user = user or os.getenv("PGUSER") or getpass.getuser()
    host = host or os.getenv("PGHOST", "localhost")
    dbname = dbname or os.getenv("PGDATABASE") or user
    port = int(port or os.getenv("PGPORT") or 5432)
    return user, host, dbname, port

def resolve_uri(
    user: str | None = None,
    host: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
) -> str:
    """
    Stuff
    """
    user, host, dbname, port = resolve_pg_connection(
        user=user, host=host, dbname=dbname, port=port
    )
    uri = f"postgres://{user}@{host}:{port}/{dbname}"
    return uri
