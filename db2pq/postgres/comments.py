# db2pq/postgres/comments.py
from __future__ import annotations

import os
import getpass
import ibis


def get_pg_comment(
    table_name: str,
    schema: str,
    *,
    user: str | None = None,
    host: str | None = None,
    database: str | None = None,
    port: int | None = None,
) -> str | None:
    """Get the comment for a PostgreSQL object (table, view, etc.)."""
    if user is None:
        user = os.getenv("PGUSER") or getpass.getuser()
    if host is None:
        host = os.getenv("PGHOST", "localhost")
    if database is None:
        database = os.getenv("PGDATABASE") or user
    if port is None:
        port = int(os.getenv("PGPORT") or 5432)

    con = ibis.postgres.connect(user=user, host=host, port=port, database=database)

    sql = """
    SELECT obj_description(
             to_regclass(%(fqname)s),
             'pg_class'
           ) AS comment
    """
    fqname = f"{schema}.{table_name}"
    cur = con.raw_sql(sql, params={"fqname": fqname})
    try:
        row = cur.fetchone()
    finally:
        cur.close()
    return row[0] if row else None


def get_wrds_comment(table_name: str, schema: str, *, wrds_id: str | None = None) -> str | None:
    if wrds_id is None:
        wrds_id = os.getenv("WRDS_ID")
        if not wrds_id:
            raise ValueError(
                "wrds_id must be provided either as an argument or via the WRDS_ID environment variable"
            )

    return get_pg_comment(
        table_name,
        schema,
        user=wrds_id,
        host="wrds-pgdata.wharton.upenn.edu",
        database="wrds",
        port=9737,
    )