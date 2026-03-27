from __future__ import annotations

from pathlib import Path
from time import perf_counter

import psycopg
import pyarrow.parquet as pq

from db2pq import close_adbc_cached, db_to_pq
from db2pq.postgres._defaults import resolve_pg_connection


MiB = 1024 * 1024
DATA_DIR = Path("/tmp/db2pq_local_bench")
HOST = "localhost"
PORT = 5432
USER = None
ROW_GROUP_SIZE = 250_000
_, _, DATABASE, _ = resolve_pg_connection(user=USER, host=HOST, dbname=None, port=PORT)

TABLE_CASES = [
    {"schema": "comp", "table_name": "company", "obs": 50_000, "adbc_reps": 2},
    {"schema": "comp", "table_name": "funda", "obs": 100_000, "adbc_reps": 1},
    {"schema": "crsp", "table_name": "dsf", "obs": 1_000_000, "adbc_reps": 1},
]


def _db_to_pq_case(case: dict, *, engine_case: dict, run_label: str) -> None:
    start = perf_counter()
    path = db_to_pq(
        table_name=case["table_name"],
        schema=case["schema"],
        user=USER,
        host=HOST,
        database=DATABASE,
        port=PORT,
        data_dir=DATA_DIR,
        obs=case["obs"],
        row_group_size=ROW_GROUP_SIZE,
        alt_table_name=f"{case['schema']}_{case['table_name']}_{run_label}",
        **engine_case,
    )
    elapsed = perf_counter() - start
    meta = pq.read_metadata(path)
    size_mb = Path(path).stat().st_size / MiB
    print(
        f"{case['schema']}.{case['table_name']}",
        run_label,
        engine_case,
        f"time={elapsed:.2f}s",
        f"rows={meta.num_rows}",
        f"size_mb={size_mb:.1f}",
    )


def _pg_parquet_case(case: dict) -> None:
    server_path = f"/tmp/db2pq_pg_parquet_{case['schema']}_{case['table_name']}_{case['obs']}.parquet"
    sql = (
        f"COPY (SELECT * FROM {case['schema']}.{case['table_name']} "
        f"LIMIT {int(case['obs'])}) "
        f"TO '{server_path}' WITH (FORMAT 'parquet')"
    )
    uri = f"postgresql://{HOST}:{PORT}/{DATABASE}" if USER is None else f"postgresql://{USER}@{HOST}:{PORT}/{DATABASE}"
    with psycopg.connect(uri) as conn:
        start = perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute("SELECT (pg_stat_file(%s)).size", (server_path,))
            size_bytes = cur.fetchone()[0]
        elapsed = perf_counter() - start

    print(
        f"{case['schema']}.{case['table_name']}",
        "pg_parquet",
        {"engine": "pg_parquet"},
        f"time={elapsed:.2f}s",
        f"rows={case['obs']}",
        f"size_mb={size_bytes / MiB:.1f}",
    )


close_adbc_cached()

for case in TABLE_CASES:
    print(f"=== {case['schema']}.{case['table_name']} obs={case['obs']} ===")
    adbc_case = {
        "engine": "adbc",
        "numeric_mode": "float64",
        "adbc_batch_size_hint_bytes": 16 * MiB,
        "adbc_use_copy": True,
    }
    for rep in range(1, case["adbc_reps"] + 1):
        _db_to_pq_case(case, engine_case=adbc_case, run_label=f"adbc_{rep}")
    _db_to_pq_case(
        case,
        engine_case={"engine": "duckdb", "batched": True},
        run_label="duckdb",
    )
    _pg_parquet_case(case)
    print()
