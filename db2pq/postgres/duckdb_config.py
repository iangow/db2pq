import os


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def configure_duckdb_connection(con) -> None:
    home_dir = os.getenv("DB2PQ_DUCKDB_HOME")
    temp_dir = os.getenv("DB2PQ_DUCKDB_TEMP_DIRECTORY")

    if home_dir:
        con.execute(f"SET home_directory='{_sql_quote(home_dir)}'")
    if temp_dir:
        con.execute(f"SET temp_directory='{_sql_quote(temp_dir)}'")

    # Required for very large text columns/aggregates that exceed Arrow's
    # regular 2 GiB string buffer limit.
    con.execute("SET arrow_large_buffer_size=true")
