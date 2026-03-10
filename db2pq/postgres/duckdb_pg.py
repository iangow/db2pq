import ibis
from .column_filter import filter_columns
from .duckdb_config import configure_duckdb_connection

def _quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'

def apply_where_sql(df, *, con, table_name: str, schema: str | None = None, where=None):
    if not where:
        return df
    if schema:
        table_ident = f"{_quote_ident(schema)}.{_quote_ident(table_name)}"
    else:
        table_ident = _quote_ident(table_name)
    return con.sql(f"SELECT * FROM {table_ident} WHERE {where}")

def apply_keep_drop(df, *, keep=None, drop=None):
    cols = filter_columns(df.columns, keep=keep, drop=drop)
    if cols != list(df.columns):
        df = df.select(*cols)
    return df

def _ensure_postgres_extension(con):
    from duckdb_extensions import import_extension

    import_extension("postgres_scanner")
    run_sql = con.raw_sql if hasattr(con, "raw_sql") else con.execute
    run_sql("LOAD postgres_scanner")

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
    configure_duckdb_connection(con)
    _ensure_postgres_extension(con)
    if threads:
        con.raw_sql(f"SET threads TO {int(threads)};")

    uri = f"postgres://{user}@{host}:{port}/{database}"
    df = con.read_postgres(uri, table_name=table_name, database=schema)
    df = apply_where_sql(df, con=con, table_name=table_name, where=where)
    return apply_keep_drop(df, keep=keep, drop=drop)
