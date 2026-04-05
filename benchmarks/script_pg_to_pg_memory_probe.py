from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

MiB = 1024 * 1024
HERE = Path(__file__).resolve().parent
DEFAULT_USER = os.getenv("PGUSER") or os.getenv("USER")
SUPPORTED_WRITERS = ("db_to_pg", "postgres_write_pg", "wrds_update_pg")


CHILD_CODE = r"""
from __future__ import annotations

import gc
import json
import os
import subprocess
import uuid

import psycopg

from db2pq import db_to_pg
from db2pq.postgres.update import postgres_write_pg
from db2pq.postgres.update import wrds_update_pg


MiB = 1024 * 1024
payload = json.loads(__PAYLOAD__)
cases = payload["cases"]
source = payload["source"]
dest = payload["dest"]
writer_name = payload["writer"]
iterations = int(payload["iterations"])


def current_rss_mb() -> float:
    try:
        import psutil
    except ImportError:
        output = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            text=True,
        ).strip()
        return int(output) / 1024.0
    return psutil.Process().memory_info().rss / MiB


def uri(cfg: dict[str, object]) -> str:
    return (
        f"postgresql://{cfg['user']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def drop_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    conn.commit()


src_uri = uri(source)
dst_uri = uri(dest)
results = []

with psycopg.connect(dst_uri) as dst_conn:
    if writer_name == "wrds_update_pg":
        import db2pq.postgres.update as update_mod

        update_mod.get_wrds_uri = lambda wrds_id=None: src_uri
        update_mod.get_wrds_conn = lambda wrds_id=None: psycopg.connect(src_uri)

    for case in cases:
        alt_table_name = f"{case['table_name']}_memprobe_{uuid.uuid4().hex[:8]}"
        schema = case["schema"]
        table_name = case["table_name"]
        obs = case["obs"]

        for iteration in range(1, iterations + 1):
            rss_before_mb = current_rss_mb()
            if writer_name == "db_to_pg":
                db_to_pg(
                    table_name,
                    schema,
                    user=source["user"],
                    host=source["host"],
                    database=source["database"],
                    port=source["port"],
                    dst_user=dest["user"],
                    dst_host=dest["host"],
                    dst_database=dest["database"],
                    dst_port=dest["port"],
                    obs=obs,
                    alt_table_name=alt_table_name,
                    create_roles=False,
                )
            elif writer_name == "postgres_write_pg":
                postgres_write_pg(
                    table_name,
                    schema,
                    src_uri=src_uri,
                    dst_uri=dst_uri,
                    obs=obs,
                    alt_table_name=alt_table_name,
                    create_roles=False,
                )
            elif writer_name == "wrds_update_pg":
                wrds_update_pg(
                    table_name,
                    schema,
                    user=dest["user"],
                    host=dest["host"],
                    dbname=dest["database"],
                    port=dest["port"],
                    obs=obs,
                    alt_table_name=alt_table_name,
                    create_roles=False,
                    force=True,
                )
            else:
                raise ValueError(f"Unsupported writer: {writer_name}")

            with dst_conn.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM "{schema}"."{alt_table_name}"')
                row_count = cur.fetchone()[0]

            drop_table(dst_conn, schema, alt_table_name)
            gc.collect()
            rss_after_mb = current_rss_mb()
            results.append(
                {
                    "writer": writer_name,
                    "label": case["label"],
                    "schema": schema,
                    "table_name": table_name,
                    "obs": obs,
                    "iteration": iteration,
                    "rows": row_count,
                    "rss_before_mb": rss_before_mb,
                    "rss_after_mb": rss_after_mb,
                    "rss_delta_mb": rss_after_mb - rss_before_mb,
                }
            )

print(json.dumps(results))
"""


def _pid_rss_bytes(pid: int) -> int:
    try:
        output = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except subprocess.CalledProcessError:
        return 0
    if not output:
        return 0
    return int(output) * 1024


def process_tree_rss(pid: int) -> int:
    try:
        output = subprocess.check_output(
            ["pgrep", "-P", str(pid)],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        child_pids = [int(line) for line in output.splitlines() if line.strip()]
    except subprocess.CalledProcessError:
        child_pids = []

    rss = _pid_rss_bytes(pid)
    for child_pid in child_pids:
        rss += process_tree_rss(child_pid)
    return rss


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_opt_int(name: str, default: int | None) -> int | None:
    value = os.getenv(name)
    if value is None:
        return default
    if value.lower() == "none":
        return None
    return int(value)


def build_cases() -> list[dict[str, object]]:
    funda_obs = env_opt_int("DB2PQ_MEMPROBE_FUNDA_OBS", None)
    dsf_obs = env_opt_int("DB2PQ_MEMPROBE_DSF_OBS", None)
    return [
        {
            "label": "comp_funda",
            "schema": "comp",
            "table_name": "funda",
            "obs": funda_obs,
        },
        {
            "label": "crsp_dsf",
            "schema": "crsp",
            "table_name": "dsf",
            "obs": dsf_obs,
        },
    ]


def run_probe(*, writer: str, iterations: int) -> dict[str, object]:
    payload = {
        "writer": writer,
        "iterations": iterations,
        "cases": build_cases(),
        "source": {
            "user": os.getenv("DB2PQ_MEMPROBE_SRC_USER", DEFAULT_USER),
            "host": os.getenv("DB2PQ_MEMPROBE_SRC_HOST", "localhost"),
            "port": env_int("DB2PQ_MEMPROBE_SRC_PORT", 5432),
            "database": os.getenv("DB2PQ_MEMPROBE_SRC_DB", "igow"),
        },
        "dest": {
            "user": os.getenv("DB2PQ_MEMPROBE_DST_USER", DEFAULT_USER),
            "host": os.getenv("DB2PQ_MEMPROBE_DST_HOST", "localhost"),
            "port": env_int("DB2PQ_MEMPROBE_DST_PORT", 5433),
            "database": os.getenv("DB2PQ_MEMPROBE_DST_DB", "test"),
        },
    }
    child_code = CHILD_CODE.replace("__PAYLOAD__", repr(json.dumps(payload)))
    cmd = [sys.executable, "-c", child_code]
    proc = subprocess.Popen(
        cmd,
        cwd=HERE.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    peak_rss = 0

    while proc.poll() is None:
        peak_rss = max(peak_rss, process_tree_rss(proc.pid))
        time.sleep(0.05)

    peak_rss = max(peak_rss, process_tree_rss(proc.pid))
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"Probe for {writer} failed with code {proc.returncode}\n"
            f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )

    records = json.loads(stdout.strip().splitlines()[-1])
    return {
        "writer": writer,
        "peak_rss_mb": peak_rss / MiB,
        "records": records,
    }


def main() -> None:
    # Key knobs:
    # - DB2PQ_MEMPROBE_WRITERS=db_to_pg,postgres_write_pg,wrds_update_pg
    # - DB2PQ_MEMPROBE_ITERATIONS=2
    # - DB2PQ_MEMPROBE_FUNDA_OBS=300000
    # - DB2PQ_MEMPROBE_DSF_OBS=1000000
    iterations = env_int("DB2PQ_MEMPROBE_ITERATIONS", 2)
    writers = [
        item.strip()
        for item in os.getenv(
            "DB2PQ_MEMPROBE_WRITERS",
            ",".join(SUPPORTED_WRITERS),
        ).split(",")
        if item.strip()
    ]
    invalid_writers = [writer for writer in writers if writer not in SUPPORTED_WRITERS]
    if invalid_writers:
        raise ValueError(
            f"Unsupported writers: {invalid_writers}. "
            f"Expected a subset of {SUPPORTED_WRITERS}."
        )

    for writer in writers:
        print(f"starting writer={writer} iterations={iterations}", flush=True)
        result = run_probe(writer=writer, iterations=iterations)
        print(
            f"writer={writer}",
            f"peak_rss_mb={result['peak_rss_mb']:.1f}",
            flush=True,
        )
        for record in result["records"]:
            print(
                f"  {record['label']}",
                f"iter={record['iteration']}",
                f"rows={record['rows']}",
                f"rss_before_mb={record['rss_before_mb']:.1f}",
                f"rss_after_mb={record['rss_after_mb']:.1f}",
                f"rss_delta_mb={record['rss_delta_mb']:.1f}",
                flush=True,
            )


if __name__ == "__main__":
    main()
