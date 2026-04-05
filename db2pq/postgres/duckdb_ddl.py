import duckdb

def create_table_from_select_duckdb(
    *,
    select_sql: str,
    wrds_uri: str,
    dst_uri: str,
    dst_schema: str,
    dst_table: str,
    drop_if_exists: bool = True,
):
    con = duckdb.connect()
    con.install_extension("postgres")
    con.load_extension("postgres")

    # Attach source and destination
    con.execute(
        f"ATTACH '{wrds_uri}' AS wrds (TYPE postgres, SCHEMA '{dst_schema}')"
    )
    con.execute(
        f"ATTACH '{dst_uri}' AS pg (TYPE postgres, SCHEMA '{dst_schema}')"
    )

    if drop_if_exists:
        con.execute(f"DROP TABLE IF EXISTS pg.{dst_schema}.{dst_table}")

    # IMPORTANT: the SELECT must reference wrds.<schema>.<table>
    create_sql = f"""
    CREATE TABLE pg.{dst_schema}.{dst_table}
    AS FROM ({select_sql})
    WITH NO DATA
    """

    con.execute(create_sql)
    con.close()
