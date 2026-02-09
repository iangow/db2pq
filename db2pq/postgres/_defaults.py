from __future__ import annotations

import os
import getpass

def resolve_pg_connection(
    *,
    user: str | None = None,
    host: str | None = None,
    database: str | None = None,
    port: int | None = None,
) -> tuple[str, str, str, int]:
    user = user or os.getenv("PGUSER") or getpass.getuser()
    host = host or os.getenv("PGHOST", "localhost")
    database = database or os.getenv("PGDATABASE") or user
    port = int(port or os.getenv("PGPORT") or 5432)
    return user, host, database, port