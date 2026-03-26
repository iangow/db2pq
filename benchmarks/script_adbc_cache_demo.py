from pathlib import Path
from time import perf_counter

import pyarrow.parquet as pq

from db2pq import close_adbc_cached, wrds_update_pq
from db2pq.files.paths import get_pq_file


MiB = 1024 * 1024
WRDS_ID = "iangow"

TABLE_CASES = [
    {
        "table_name": "ccmxpf_lnkhist",
        "schema": "crsp",
        "col_types": {"lpermno": "integer", "lpermco": "integer"},
    },
    {
        "table_name": "stocknames",
        "schema": "crsp",
    },
    {
        "table_name": "dsi",
        "schema": "crsp",
    },
]


def run_case(pass_name: str, engine_case: dict[str, object]) -> None:
    print(f"=== {pass_name} {engine_case['name']} ===")
    for case in TABLE_CASES:
        case_kwargs = dict(case)
        if engine_case["name"] == "duckdb":
            case_kwargs.pop("col_types", None)

        alt_table_name = f"{case['table_name']}_pass1_{engine_case['name']}"
        start = perf_counter()
        wrds_update_pq(
            wrds_id=WRDS_ID,
            row_group_size=250_000,
            alt_table_name=alt_table_name,
            **engine_case["kwargs"],
            **case_kwargs,
        )
        elapsed = perf_counter() - start
        pq_file = Path(
            get_pq_file(table_name=alt_table_name, schema=case["schema"])
        )
        rows = pq.read_metadata(pq_file).num_rows
        size_mb = pq_file.stat().st_size / (1024 * 1024)
        print(
            f"{case['schema']}.{case['table_name']}",
            f"time={elapsed:.2f}s",
            f"rows={rows}",
            f"size_mb={size_mb:.1f}",
        )
    print()


DUCKDB_CASE = {
    "name": "duckdb",
    "kwargs": {
        "engine": "duckdb",
        "batched": True,
        "force": True,
    },
}

ADBC_CASE = {
    "name": "adbc",
    "kwargs": {
        "engine": "adbc",
        "numeric_mode": "float64",
        "adbc_batch_size_hint_bytes": 16 * MiB,
        "adbc_use_copy": True,
        "force": True,
    },
}

ADBC_CASE_NO_FORCE = {
    "name": "adbc",
    "kwargs": {
        "engine": "adbc",
        "numeric_mode": "float64",
        "adbc_batch_size_hint_bytes": 16 * MiB,
        "adbc_use_copy": True,
        "force": False,
    },
}


close_adbc_cached()
run_case("pass1", DUCKDB_CASE)
run_case("pass1", ADBC_CASE)
run_case("pass2", ADBC_CASE_NO_FORCE)
close_adbc_cached()
