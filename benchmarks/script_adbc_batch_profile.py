from __future__ import annotations

from statistics import mean
from time import perf_counter

import adbc_driver_postgresql
import adbc_driver_postgresql.dbapi as adbc_dbapi

from db2pq.postgres.adbc import _merge_adbc_col_types
from db2pq.postgres.comments import get_pg_conn
from db2pq.postgres.introspect import (
    get_table_column_types,
    get_table_columns,
    get_table_numeric_bounds,
)
from db2pq.postgres.select_sql import build_wrds_select_sql


MiB = 1024 * 1024

CASES = [
    {
        "label": "dsf_3m",
        "uri": "postgresql://iangow@localhost:5432/iangow",
        "schema": "crsp",
        "table": "dsf",
        "obs": 3_000_000,
        "numeric_mode": "float64",
        "adbc_batch_size_hint_bytes": 16 * MiB,
    },
    {
        "label": "funda_300k",
        "uri": "postgresql://iangow@localhost:5432/iangow",
        "schema": "comp",
        "table": "funda",
        "obs": 300_000,
        "numeric_mode": "float64",
        "adbc_batch_size_hint_bytes": 16 * MiB,
    },
]


for case in CASES:
    print(f"=== {case['label']} ===")

    with get_pg_conn(case["uri"]) as conn:
        all_cols = get_table_columns(conn, case["schema"], case["table"])
        source_col_types = get_table_column_types(conn, case["schema"], case["table"])
        numeric_bounds = get_table_numeric_bounds(conn, case["schema"], case["table"])
        col_types = _merge_adbc_col_types(
            None,
            numeric_bounds,
            numeric_mode=case["numeric_mode"],
        )
        sql = build_wrds_select_sql(
            conn=conn,
            schema=case["schema"],
            table=case["table"],
            columns=all_cols,
            col_types=col_types,
            source_col_types=source_col_types,
            tz="UTC",
            obs=case["obs"],
            where=None,
        )

    start = perf_counter()
    with adbc_dbapi.connect(case["uri"]) as adbc_conn:
        with adbc_conn.cursor() as cur:
            cur._stmt.set_options(
                **{
                    adbc_driver_postgresql.StatementOptions.BATCH_SIZE_HINT_BYTES.value:
                    int(case["adbc_batch_size_hint_bytes"]),
                    adbc_driver_postgresql.StatementOptions.USE_COPY.value:
                    "true",
                }
            )
            cur.execute(sql)
            reader = cur.fetch_record_batch()

            batch_rows = []
            batch_bytes = []
            total_rows = 0

            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                rows = batch.num_rows
                bytes_ = batch.nbytes
                batch_rows.append(rows)
                batch_bytes.append(bytes_)
                total_rows += rows

    elapsed = perf_counter() - start
    print(f"time={elapsed:.2f}s")
    print(f"num_batches={len(batch_rows)}")
    print(f"total_rows={total_rows}")
    print(f"min_batch_rows={min(batch_rows)}")
    print(f"max_batch_rows={max(batch_rows)}")
    print(f"avg_batch_rows={mean(batch_rows):.1f}")
    print(f"min_batch_mb={min(batch_bytes) / MiB:.2f}")
    print(f"max_batch_mb={max(batch_bytes) / MiB:.2f}")
    print(f"avg_batch_mb={mean(batch_bytes) / MiB:.2f}")
    print()
