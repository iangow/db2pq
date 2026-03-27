from __future__ import annotations

import gc
import os
import subprocess
import uuid

import psycopg
import pyarrow.parquet as pq
import pyarrow.types as pat
import pytest

from db2pq import db_to_pg, db_to_pq, process_sql, set_table_comment
from db2pq.postgres.comments import get_pg_comment
from db2pq.postgres.update import postgres_write_pg
from db2pq.postgres.update import wrds_update_pg


def _uri(*, user: str, host: str, port: int, dbname: str) -> str:
    return f"postgresql://{user}@{host}:{port}/{dbname}"


def _drop_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    conn.commit()


def _current_rss_mb() -> float:
    try:
        import psutil
    except ImportError:
        output = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            text=True,
        ).strip()
        return int(output) / 1024.0

    return psutil.Process().memory_info().rss / (1024 * 1024)


def test_db_to_pq_duckdb_local_small_table(pg_test_config, data_dir, require_source_table):
    require_source_table("comp", "company")

    pq_file = db_to_pq(
        table_name="company",
        schema="comp",
        user=pg_test_config["user"],
        host=pg_test_config["host"],
        database=pg_test_config["src_db"],
        port=pg_test_config["port"],
        data_dir=data_dir,
        alt_table_name="company_duckdb_test",
        obs=250,
        engine="duckdb",
        batched=True,
    )

    meta = pq.read_metadata(pq_file)
    assert meta.num_rows == 250


def test_db_to_pq_adbc_local_small_table(pg_test_config, data_dir, require_source_table):
    require_source_table("crsp", "dsi")

    pq_file = db_to_pq(
        table_name="dsi",
        schema="crsp",
        user=pg_test_config["user"],
        host=pg_test_config["host"],
        database=pg_test_config["src_db"],
        port=pg_test_config["port"],
        data_dir=data_dir,
        alt_table_name="dsi_adbc_test",
        obs=500,
        engine="adbc",
        numeric_mode="float64",
        adbc_use_copy=True,
        adbc_batch_size_hint_bytes=8 * 1024 * 1024,
        row_group_size=250_000,
    )

    meta = pq.read_metadata(pq_file)
    assert meta.num_rows == 500


def test_db_to_pq_adbc_preserves_timestamp_column(
    pg_test_config,
    data_dir,
    require_source_table,
):
    require_source_table("public", "example")

    pq_file = db_to_pq(
        table_name="example",
        schema="public",
        user=pg_test_config["user"],
        host=pg_test_config["host"],
        database=pg_test_config["src_db"],
        port=pg_test_config["port"],
        data_dir=data_dir,
        alt_table_name="example_adbc_timestamp_test",
        engine="adbc",
        numeric_mode="float64",
        adbc_use_copy=True,
        adbc_batch_size_hint_bytes=8 * 1024 * 1024,
        row_group_size=250_000,
    )

    table = pq.read_table(pq_file)
    ts_field = table.schema.field("ts")
    assert pat.is_timestamp(ts_field.type)
    assert ts_field.type.tz is not None
    assert table.num_rows == 71


def test_wrds_update_pg_normalizes_arrow_style_col_types(
    monkeypatch,
    pg_test_config,
    require_source_table,
):
    require_source_table("crsp", "ccmxpf_lnkhist")

    src_uri = _uri(
        user=pg_test_config["user"],
        host=pg_test_config["host"],
        port=pg_test_config["port"],
        dbname=pg_test_config["src_db"],
    )
    dst_uri = _uri(
        user=pg_test_config["user"],
        host=pg_test_config["host"],
        port=pg_test_config["port"],
        dbname=pg_test_config["dst_db"],
    )

    import db2pq.postgres.update as update_mod

    monkeypatch.setattr(update_mod, "get_wrds_uri", lambda wrds_id=None: src_uri)
    monkeypatch.setattr(update_mod, "get_wrds_conn", lambda wrds_id=None: psycopg.connect(src_uri))

    alt_table_name = f"ccmxpf_lnkhist_test_{uuid.uuid4().hex[:8]}"

    with psycopg.connect(dst_uri) as dst_conn:
        _drop_table(dst_conn, "crsp", alt_table_name)

        wrds_update_pg(
            "ccmxpf_lnkhist",
            "crsp",
            user=pg_test_config["user"],
            host=pg_test_config["host"],
            dbname=pg_test_config["dst_db"],
            port=pg_test_config["port"],
            obs=250,
            alt_table_name=alt_table_name,
            col_types={"lpermno": "int32", "lpermco": "int32"},
            create_roles=False,
            force=True,
        )

        with dst_conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = %s
                """,
                ("crsp", alt_table_name, "lpermno"),
            )
            assert cur.fetchone()[0] == "integer"

            cur.execute(
                f'SELECT count(*) FROM "crsp"."{alt_table_name}"'
            )
            assert cur.fetchone()[0] == 250

        _drop_table(dst_conn, "crsp", alt_table_name)


def test_postgres_write_pg_local_small_table(pg_test_config, dst_pg_conn, require_source_table):
    require_source_table("crsp", "dsi")

    src_uri = _uri(
        user=pg_test_config["src_user"],
        host=pg_test_config["src_host"],
        port=pg_test_config["src_port"],
        dbname=pg_test_config["src_db"],
    )
    dst_uri = _uri(
        user=pg_test_config["dst_user"],
        host=pg_test_config["dst_host"],
        port=pg_test_config["dst_port"],
        dbname=pg_test_config["dst_db"],
    )
    alt_table_name = f"dsi_pg_copy_test_{uuid.uuid4().hex[:8]}"

    try:
        postgres_write_pg(
            "dsi",
            "crsp",
            src_uri=src_uri,
            dst_uri=dst_uri,
            obs=500,
            alt_table_name=alt_table_name,
            create_roles=False,
        )

        with dst_pg_conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "crsp"."{alt_table_name}"')
            assert cur.fetchone()[0] == 500
    finally:
        _drop_table(dst_pg_conn, "crsp", alt_table_name)


def test_db_to_pg_local_small_table(pg_test_config, dst_pg_conn, require_source_table):
    require_source_table("crsp", "dsi")

    alt_table_name = f"dsi_db_to_pg_test_{uuid.uuid4().hex[:8]}"

    try:
        db_to_pg(
            "dsi",
            "crsp",
            user=pg_test_config["src_user"],
            host=pg_test_config["src_host"],
            database=pg_test_config["src_db"],
            port=pg_test_config["src_port"],
            dst_user=pg_test_config["dst_user"],
            dst_host=pg_test_config["dst_host"],
            dst_database=pg_test_config["dst_db"],
            dst_port=pg_test_config["dst_port"],
            obs=500,
            alt_table_name=alt_table_name,
            create_roles=False,
        )

        with dst_pg_conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "crsp"."{alt_table_name}"')
            assert cur.fetchone()[0] == 500
    finally:
        _drop_table(dst_pg_conn, "crsp", alt_table_name)


def test_process_sql_creates_index(pg_test_config, dst_pg_conn):
    schema = "public"
    table = f"process_sql_test_{uuid.uuid4().hex[:8]}"
    index = f"{table}_id_idx"

    try:
        with dst_pg_conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{schema}"."{table}" (id integer)')
        dst_pg_conn.commit()

        status = process_sql(
            f'CREATE INDEX "{index}" ON "{schema}"."{table}" (id)',
            user=pg_test_config["dst_user"],
            host=pg_test_config["dst_host"],
            dbname=pg_test_config["dst_db"],
            port=pg_test_config["dst_port"],
        )

        assert status == "CREATE INDEX"

        with dst_pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = %s
                  AND tablename = %s
                  AND indexname = %s
                """,
                (schema, table, index),
            )
            assert cur.fetchone() == (1,)
    finally:
        with dst_pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        dst_pg_conn.commit()


def test_set_table_comment_uses_destination_defaults(pg_test_config, dst_pg_conn):
    schema = "public"
    table = f"set_comment_test_{uuid.uuid4().hex[:8]}"
    comment = "comment set by db2pq"

    try:
        with dst_pg_conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{schema}"."{table}" (id integer)')
        dst_pg_conn.commit()

        set_table_comment(
            schema=schema,
            table_name=table,
            comment=comment,
            user=pg_test_config["dst_user"],
            host=pg_test_config["dst_host"],
            dbname=pg_test_config["dst_db"],
            port=pg_test_config["dst_port"],
        )

        assert get_pg_comment(
            table_name=table,
            schema=schema,
            user=pg_test_config["dst_user"],
            host=pg_test_config["dst_host"],
            dbname=pg_test_config["dst_db"],
            port=pg_test_config["dst_port"],
        ) == comment
    finally:
        with dst_pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        dst_pg_conn.commit()


@pytest.mark.skipif(
    os.getenv("DB2PQ_ENABLE_MEMORY_TEST") != "1",
    reason="Set DB2PQ_ENABLE_MEMORY_TEST=1 to run the local memory regression test.",
)
def test_postgres_write_pg_memory_regression(pg_test_config, dst_pg_conn, require_source_table):
    source_schema = os.getenv("DB2PQ_MEMORY_TEST_SCHEMA", "crsp")
    source_table = os.getenv("DB2PQ_MEMORY_TEST_TABLE", "dsf")
    obs = int(os.getenv("DB2PQ_MEMORY_TEST_OBS", "250000"))
    iterations = int(os.getenv("DB2PQ_MEMORY_TEST_ITERATIONS", "3"))
    max_growth_mb = float(os.getenv("DB2PQ_MEMORY_TEST_MAX_GROWTH_MB", "200"))

    require_source_table(source_schema, source_table)

    src_uri = _uri(
        user=pg_test_config["src_user"],
        host=pg_test_config["src_host"],
        port=pg_test_config["src_port"],
        dbname=pg_test_config["src_db"],
    )
    dst_uri = _uri(
        user=pg_test_config["dst_user"],
        host=pg_test_config["dst_host"],
        port=pg_test_config["dst_port"],
        dbname=pg_test_config["dst_db"],
    )
    alt_table_name = f"{source_table}_mem_test_{uuid.uuid4().hex[:8]}"

    gc.collect()
    rss_before_mb = _current_rss_mb()
    try:
        for _ in range(iterations):
            postgres_write_pg(
                source_table,
                source_schema,
                src_uri=src_uri,
                dst_uri=dst_uri,
                obs=obs,
                alt_table_name=alt_table_name,
                create_roles=False,
            )
            gc.collect()

        rss_after_mb = _current_rss_mb()
        growth_mb = rss_after_mb - rss_before_mb
        assert growth_mb <= max_growth_mb, (
            f"RSS grew by {growth_mb:.1f} MB after {iterations} iterations "
            f"copying {source_schema}.{source_table} (limit {obs})"
        )
    finally:
        _drop_table(dst_pg_conn, source_schema, alt_table_name)
