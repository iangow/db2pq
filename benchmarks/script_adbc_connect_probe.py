from time import perf_counter

from db2pq.postgres.adbc import _merge_adbc_col_types
from db2pq.postgres.comments import get_pg_conn
from db2pq.postgres.introspect import (
    get_table_column_types,
    get_table_columns,
    get_table_numeric_bounds,
)
from db2pq.postgres.select_sql import build_wrds_select_sql
from db2pq.postgres.wrds import get_wrds_uri


MiB = 1024 * 1024
WRDS_ID = "iangow"
SCHEMA = "crsp"
TABLE = "dsf"
OBS = 100
ADBC_BATCH_SIZE_HINT_BYTES = 16 * MiB
ADBC_USE_COPY = True
NUMERIC_MODE = "float64"
REPS = 3


def timed(label, fn):
    start = perf_counter()
    result = fn()
    elapsed = perf_counter() - start
    print(f"{label}={elapsed:.3f}s")
    return result


for rep in range(1, REPS + 1):
    print(f"=== rep={rep} obs={OBS} ===")
    uri = get_wrds_uri(WRDS_ID)

    conn = timed("psycopg_connect", lambda: get_pg_conn(uri))
    try:
        all_cols = timed(
            "get_table_columns",
            lambda: get_table_columns(conn, SCHEMA, TABLE),
        )
        source_col_types = timed(
            "get_table_column_types",
            lambda: get_table_column_types(conn, SCHEMA, TABLE),
        )
        numeric_bounds = timed(
            "get_table_numeric_bounds",
            lambda: get_table_numeric_bounds(conn, SCHEMA, TABLE),
        )
        col_types = timed(
            "merge_col_types",
            lambda: _merge_adbc_col_types(
                None,
                numeric_bounds,
                numeric_mode=NUMERIC_MODE,
            ),
        )
        sql = timed(
            "build_sql",
            lambda: build_wrds_select_sql(
                conn=conn,
                schema=SCHEMA,
                table=TABLE,
                columns=all_cols,
                col_types=col_types,
                source_col_types=source_col_types,
                tz="UTC",
                obs=OBS,
                where=None,
            ),
        )
    finally:
        conn.close()

    import adbc_driver_postgresql
    import adbc_driver_postgresql.dbapi as adbc_dbapi

    adbc_conn = timed("adbc_connect", lambda: adbc_dbapi.connect(uri))
    try:
        cur = timed("adbc_cursor", lambda: adbc_conn.cursor())
        try:
            timed(
                "adbc_set_options",
                lambda: cur._stmt.set_options(
                    **{
                        adbc_driver_postgresql.StatementOptions.BATCH_SIZE_HINT_BYTES.value:
                        ADBC_BATCH_SIZE_HINT_BYTES,
                        adbc_driver_postgresql.StatementOptions.USE_COPY.value:
                        "true" if ADBC_USE_COPY else "false",
                    }
                ),
            )
            timed("adbc_execute", lambda: cur.execute(sql))
            reader = timed("fetch_record_batch", lambda: cur.fetch_record_batch())
            first_batch = timed("read_first_batch", lambda: reader.read_next_batch())
            print(f"first_batch_rows={first_batch.num_rows}")

            total_rows = first_batch.num_rows
            drain_start = perf_counter()
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                total_rows += batch.num_rows
            print(f"drain_remaining={(perf_counter() - drain_start):.3f}s")
            print(f"total_rows={total_rows}")
        finally:
            cur.close()
    finally:
        adbc_conn.close()

    print()
