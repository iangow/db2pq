from pathlib import Path
from time import perf_counter
import pyarrow.parquet as pq

from db2pq import wrds_pg_to_pq

MiB = 1024 * 1024

cases = [
    {"engine": "duckdb", "batched": True},
    {
        "engine": "adbc",
        "numeric_mode": "float64",
        "adbc_batch_size_hint_bytes": 16 * MiB,
        "adbc_use_copy": True,
    },
]

for obs in [100, 1_000]:
    for rep in range(1, 4):
        for case in cases:
            start = perf_counter()
            path = wrds_pg_to_pq(
                table_name="dsf",
                schema="crsp",
                wrds_id="iangow",
                obs=obs,
                row_group_size=250_000,
                alt_table_name=(
                    f"wrds_dsf_{obs}_{rep}_{case['engine']}_"
                    f"{case.get('numeric_mode', 'batched')}_"
                    f"{case.get('adbc_batch_size_hint_bytes', 'default')}_"
                    f"{case.get('adbc_use_copy', 'default')}"
                ),
                **case,
            )
            elapsed = perf_counter() - start
            table = pq.read_table(path)
            size_mb = Path(path).stat().st_size / (1024 * 1024)
            print(
                f"obs={obs}",
                f"rep={rep}",
                case,
                f"time={elapsed:.2f}s",
                f"rows={table.num_rows}",
                f"size_mb={size_mb:.1f}",
            )
