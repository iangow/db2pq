from dataclasses import dataclass

from .comments import get_pg_conn
from .introspect import get_table_column_types, get_table_columns
from .select_sql import plan_wrds_query


@dataclass
class DuckDBArrowQuery:
    connection: object
    relation: object

    def fetch_arrow_reader(self):
        return self.relation.fetch_arrow_reader()

    def fetch_arrow_table(self):
        return self.relation.fetch_arrow_table()

def read_postgres_table(
    *,
    user,
    host,
    port,
    database,
    schema,
    table_name,
    col_types=None,
    obs=None,
    threads=None,
    keep=None,
    drop=None,
    where=None,
    tz="UTC",
):
    import duckdb

    con = duckdb.connect()
    # Required for very large text columns/aggregates that exceed Arrow's
    # regular 2 GiB string buffer limit.
    con.execute("SET arrow_large_buffer_size=true;")
    if threads:
        con.execute(f"SET threads TO {int(threads)};")

    uri = f"postgres://{user}@{host}:{port}/{database}"
    with get_pg_conn(uri) as pg_conn:
        all_cols = get_table_columns(pg_conn, schema, table_name)
        source_col_types = get_table_column_types(pg_conn, schema, table_name)
        plan = plan_wrds_query(
            conn=pg_conn,
            schema=schema,
            table=table_name,
            all_cols=all_cols,
            source_col_types=source_col_types,
            col_types=col_types,
            keep=keep,
            drop=drop,
            tz=tz,
            obs=obs,
            where=where,
            qualified_alias="wrds",
        )

    con.execute(
        f"ATTACH '{uri}' AS wrds (TYPE postgres, SCHEMA '{schema}')"
    )
    relation = con.sql(plan.qualified_sql)
    return DuckDBArrowQuery(connection=con, relation=relation)
