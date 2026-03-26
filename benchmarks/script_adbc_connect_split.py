from time import perf_counter
from urllib.parse import urlencode

from db2pq.postgres.comments import get_pg_conn
from db2pq.postgres.wrds import get_wrds_uri


WRDS_ID = "iangow"
REPS = 3
BASE_URI = get_wrds_uri(WRDS_ID)

VARIANTS = [
    ("base", {}),
    ("sslmode_require", {"sslmode": "require"}),
    ("gssenc_disable", {"gssencmode": "disable"}),
]


def add_params(uri: str, params: dict[str, str]) -> str:
    if not params:
        return uri
    return f"{uri}?{urlencode(params)}"


def timed(fn):
    start = perf_counter()
    result = fn()
    elapsed = perf_counter() - start
    return result, elapsed


for rep in range(1, REPS + 1):
    print(f"=== rep={rep} ===")
    for name, params in VARIANTS:
        uri = add_params(BASE_URI, params)
        print(f"-- {name} --")

        psycopg_conn, elapsed = timed(lambda: get_pg_conn(uri))
        print(f"psycopg_connect={elapsed:.3f}s")
        psycopg_conn.close()

        import adbc_driver_manager
        import adbc_driver_postgresql

        db, elapsed = timed(lambda: adbc_driver_postgresql.connect(uri))
        print(f"adbc_database_connect={elapsed:.3f}s")

        try:
            conn, elapsed = timed(lambda: adbc_driver_manager.AdbcConnection(db))
            print(f"adbc_connection_init={elapsed:.3f}s")
            conn.close()
        finally:
            db.close()

        print()
