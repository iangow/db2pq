import ibis
from .column_filter import filter_columns

def _quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

def apply_where_sql(df, *, con, table_name: str, where=None):
    if not where:
        return df
    table_ident = _quote_ident(table_name)
    return con.sql(f"SELECT * FROM {table_ident} WHERE {where}")

def apply_keep_drop(df, *, keep=None, drop=None):
    cols = filter_columns(df.columns, keep=keep, drop=drop)
    if cols != list(df.columns):
        df = df.select(*cols)
    return df

def read_postgres_table(
    *,
    user,
    host,
    port,
    database,
    schema,
    table_name,
    threads=None,
    keep=None,
    drop=None,
    where=None,
):
    con = ibis.duckdb.connect()
    # Required for very large text columns/aggregates that exceed Arrow's
    # regular 2 GiB string buffer limit.
    con.raw_sql("SET arrow_large_buffer_size=true;")
    if threads:
        con.raw_sql(f"SET threads TO {int(threads)};")

    uri = f"postgres://{user}@{host}:{port}/{database}"
    df = con.read_postgres(uri, table_name=table_name, database=schema)
    df = apply_where_sql(df, con=con, table_name=table_name, where=where)

    df = apply_keep_drop(df, keep=keep, drop=drop)
    return df
