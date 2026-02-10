import ibis

import ibis.selectors as s

def apply_keep_drop(df, *, keep=None, drop=None):
    if drop:
        df = df.drop(s.matches(drop))
    if keep:
        df = df.select(s.matches(keep))
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
):
    con = ibis.duckdb.connect()
    if threads:
        con.raw_sql(f"SET threads TO {int(threads)};")

    uri = f"postgres://{user}@{host}:{port}/{database}"
    df = con.read_postgres(uri, table_name=table_name, database=schema)

    df = apply_keep_drop(df, keep=keep, drop=drop)
    return df
