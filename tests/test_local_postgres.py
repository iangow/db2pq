from __future__ import annotations

import uuid

import psycopg
import pyarrow.parquet as pq
import pyarrow.types as pat

from db2pq import db_to_pq
from db2pq.postgres.update import wrds_update_pg


def _uri(*, user: str, host: str, port: int, dbname: str) -> str:
    return f"postgresql://{user}@{host}:{port}/{dbname}"


def _drop_table(conn, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    conn.commit()


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
