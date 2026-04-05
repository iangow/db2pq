from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil


MiB = 1024 * 1024
HERE = Path(__file__).resolve().parent

TABLE_CASES = [
    {
        "label": "dsf_3m_adbc",
        "table_name": "dsf",
        "schema": "crsp",
        "database": "iangow",
        "obs": 3_000_000,
        "kwargs": {
            "engine": "adbc",
            "numeric_mode": "float64",
            "adbc_batch_size_hint_bytes": 16 * MiB,
            "adbc_use_copy": True,
        },
    },
    {
        "label": "funda_300k_adbc",
        "table_name": "funda",
        "schema": "comp",
        "database": "iangow",
        "obs": 300_000,
        "kwargs": {
            "engine": "adbc",
            "numeric_mode": "float64",
            "adbc_batch_size_hint_bytes": 16 * MiB,
            "adbc_use_copy": True,
        },
    },
]


CHILD_CODE = r"""
from pathlib import Path
from time import perf_counter
import json
import pyarrow.parquet as pq

from db2pq import db_to_pq

case = json.loads({payload})
start = perf_counter()
path = db_to_pq(
    table_name=case["table_name"],
    schema=case["schema"],
    database=case["database"],
    obs=case["obs"],
    row_group_size=250_000,
    alt_table_name=case["label"],
    **case["kwargs"],
)
elapsed = perf_counter() - start
meta = pq.read_metadata(path)
size_mb = Path(path).stat().st_size / (1024 * 1024)
print(json.dumps({{
    "path": path,
    "elapsed": elapsed,
    "rows": meta.num_rows,
    "size_mb": size_mb,
}}))
"""


def process_tree_rss(proc: psutil.Process) -> int:
    rss = 0
    try:
        rss += proc.memory_info().rss
    except psutil.Error:
        return 0

    for child in proc.children(recursive=True):
        try:
            rss += child.memory_info().rss
        except psutil.Error:
            pass
    return rss


def run_case(case: dict[str, object]) -> dict[str, object]:
    payload = json.dumps(case)
    cmd = [
        sys.executable,
        "-c",
        CHILD_CODE.format(payload=repr(payload)),
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=HERE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    ps_proc = psutil.Process(proc.pid)
    peak_rss = 0

    while proc.poll() is None:
        peak_rss = max(peak_rss, process_tree_rss(ps_proc))
        time.sleep(0.05)

    peak_rss = max(peak_rss, process_tree_rss(ps_proc))
    stdout, stderr = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(
            f"Case {case['label']} failed with code {proc.returncode}\n"
            f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )

    result = json.loads(stdout.strip().splitlines()[-1])
    result["peak_rss_mb"] = peak_rss / MiB
    result["label"] = case["label"]
    return result


for case in TABLE_CASES:
    print(f"starting {case['label']}", flush=True)
    result = run_case(case)
    print(
        result["label"],
        f"time={result['elapsed']:.2f}s",
        f"rows={result['rows']}",
        f"size_mb={result['size_mb']:.1f}",
        f"peak_rss_mb={result['peak_rss_mb']:.1f}",
        flush=True,
    )
