from __future__ import annotations

import os
import ibis

from ._defaults import resolve_pg_connection


def get_pg_comment_con(con, *, schema: str, table_name: str) -> str | None:
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


def get_pg_comment(
    table_name: str,
    schema: str,
    *,
    user: str | None = None,
    host: str | None = None,
    database: str | None = None,
    port: int | None = None,
) -> str | None:
    user, host, database, port = resolve_pg_connection(
        user=user, host=host, database=database, port=port
    )
    con = ibis.postgres.connect(user=user, host=host, port=port, database=database)
    return get_pg_comment_con(con, schema=schema, table_name=table_name)


def resolve_wrds_id(wrds_id: str | None = None) -> str:
    wrds_id = wrds_id or os.getenv("WRDS_ID")
    if not wrds_id:
        raise ValueError(
            "wrds_id must be provided either as an argument or "
            "via the WRDS_ID environment variable"
        )
    return wrds_id


def get_wrds_conn(wrds_id: str | None = None):
    wrds_id = resolve_wrds_id(wrds_id)
    return ibis.postgres.connect(
        user=wrds_id,
        host="wrds-pgdata.wharton.upenn.edu",
        database="wrds",
        port=9737,
    )


def get_wrds_comment(table_name: str, schema: str, *, wrds_id: str | None = None) -> str | None:
    con = get_wrds_conn(wrds_id)
    return get_pg_comment_con(con, schema=schema, table_name=table_name)
