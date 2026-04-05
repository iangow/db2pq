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
    (
        "sslmode_require_gssenc_disable",
        {"sslmode": "require", "gssencmode": "disable"},
    ),
    (
        "sslmode_require_gssenc_disable_timeout5",
        {"sslmode": "require", "gssencmode": "disable", "connect_timeout": "5"},
    ),
]


def add_params(uri: str, params: dict[str, str]) -> str:
    if not params:
        return uri
    return f"{uri}?{urlencode(params)}"


def time_connect(label: str, connect_fn) -> None:
    start = perf_counter()
    conn = connect_fn()
    elapsed = perf_counter() - start
    print(f"{label}={elapsed:.3f}s")
    conn.close()


for rep in range(1, REPS + 1):
    print(f"=== rep={rep} ===")
    for name, params in VARIANTS:
        uri = add_params(BASE_URI, params)
        print(f"-- {name} --")

        time_connect("psycopg_connect", lambda uri=uri: get_pg_conn(uri))

        import adbc_driver_postgresql.dbapi as adbc_dbapi

        time_connect("adbc_connect", lambda uri=uri: adbc_dbapi.connect(uri))
        print()
