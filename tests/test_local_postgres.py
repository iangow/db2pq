from __future__ import annotations

import gc
import os
import subprocess
import uuid

import psycopg
import pyarrow.parquet as pq
import pyarrow.types as pat
import pytest

from db2pq import db_to_pg, db_to_pq, pg_update_pq, process_sql, pq_update_pg, set_table_comment
from db2pq.files.parquet import get_modified_pq
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


def test_wrds_update_pg_imports_when_destination_table_missing(monkeypatch, capsys):
    import db2pq.credentials as credentials_mod
    import db2pq.postgres.update as update_mod

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    calls = []

    monkeypatch.setattr(update_mod, "resolve_uri", lambda **kwargs: "postgresql://dst")
    monkeypatch.setattr(update_mod, "get_wrds_uri", lambda wrds_id=None: "postgresql://src")
    monkeypatch.setattr(update_mod, "get_wrds_conn", lambda wrds_id=None: DummyConn())
    monkeypatch.setattr(update_mod, "get_pg_conn", lambda uri: DummyConn())
    monkeypatch.setattr(update_mod, "get_wrds_comment", lambda **kwargs: None)
    monkeypatch.setattr(update_mod, "_table_exists", lambda conn, schema, table_name: False)
    monkeypatch.setattr(credentials_mod, "ensure_wrds_access", lambda wrds_id=None: "user")
    monkeypatch.setattr(
        update_mod,
        "postgres_write_pg",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    assert wrds_update_pg("some_view", "boardex") is True
    assert len(calls) == 1
    assert calls[0]["table_name"] == "some_view"
    assert calls[0]["dst_schema"] == "boardex"

    out = capsys.readouterr().out
    assert "does not exist in destination" in out
    assert "Getting from WRDS." in out


def test_wrds_update_pg_use_sas_passes_sas_comment_to_writer(monkeypatch):
    import db2pq.credentials as credentials_mod
    import db2pq.postgres.update as update_mod

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    calls = []

    monkeypatch.setattr(update_mod, "resolve_uri", lambda **kwargs: "postgresql://dst")
    monkeypatch.setattr(update_mod, "get_wrds_uri", lambda wrds_id=None: "postgresql://src")
    monkeypatch.setattr(update_mod, "get_wrds_conn", lambda wrds_id=None: DummyConn())
    monkeypatch.setattr(update_mod, "get_pg_conn", lambda uri: DummyConn())
    monkeypatch.setattr(update_mod, "_table_exists", lambda conn, schema, table_name: False)
    monkeypatch.setattr(credentials_mod, "ensure_wrds_access", lambda wrds_id=None: "user")

    seen = {}

    def fake_get_wrds_comment(**kwargs):
        seen.update(kwargs)
        return "Last modified: 2026-03-27"

    monkeypatch.setattr(update_mod, "get_wrds_comment", fake_get_wrds_comment)
    monkeypatch.setattr(
        update_mod,
        "postgres_write_pg",
        lambda **kwargs: calls.append(kwargs) or True,
    )

    assert wrds_update_pg("some_view", "boardex", use_sas=True) is True
    assert seen["use_sas"] is True
    assert seen["schema"] == "boardex"
    assert calls[0]["source_comment"] == "Last modified: 2026-03-27"


def test_pg_update_pq_passes_local_comment_to_db_to_pq(monkeypatch, tmp_path):
    import db2pq.core as core_mod

    seen = {}

    monkeypatch.setattr(
        "db2pq.postgres.comments.get_pg_comment",
        lambda **kwargs: "local table (Updated 2026-03-28)",
    )
    monkeypatch.setattr(
        core_mod,
        "db_to_pq",
        lambda **kwargs: seen.update(kwargs) or str(tmp_path / "public" / "example.parquet"),
    )

    result = core_mod.pg_update_pq(
        table_name="example",
        schema="public",
        user="alice",
        host="localhost",
        database="research",
        port=5432,
        data_dir=tmp_path,
        engine="duckdb",
        where="id > 10",
    )

    assert result == str(tmp_path / "public" / "example.parquet")
    assert seen["table_name"] == "example"
    assert seen["schema"] == "public"
    assert seen["modified"] == "local table (Updated 2026-03-28)"
    assert seen["database"] == "research"
    assert seen["where"] == "id > 10"


def test_pg_update_pq_messages_when_comment_missing(monkeypatch, tmp_path, capsys):
    import db2pq.core as core_mod

    called = False

    monkeypatch.setattr("db2pq.postgres.comments.get_pg_comment", lambda **kwargs: None)

    def fake_db_to_pq(**kwargs):
        nonlocal called
        called = True
        return str(tmp_path / "public" / "example.parquet")

    monkeypatch.setattr(core_mod, "db_to_pq", fake_db_to_pq)

    result = core_mod.pg_update_pq(
        table_name="example",
        schema="public",
        user="alice",
        host="localhost",
        database="research",
        port=5432,
        data_dir=tmp_path,
    )

    assert result is None
    assert called is False
    out = capsys.readouterr().out
    assert "has no parseable last-modified comment" in out
    assert "force=True" in out


def test_pq_update_pg_passes_parquet_comment_to_writer(monkeypatch, tmp_path):
    import db2pq.postgres.update as update_mod

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    pq_file = tmp_path / "public" / "example.parquet"
    pq_file.parent.mkdir(parents=True, exist_ok=True)
    pq_file.touch()

    calls = []

    monkeypatch.setattr(update_mod, "resolve_uri", lambda **kwargs: "postgresql://dst")
    monkeypatch.setattr(update_mod, "get_pg_conn", lambda uri: DummyConn())
    monkeypatch.setattr(update_mod, "_table_exists", lambda conn, schema, table_name: False)
    monkeypatch.setattr("db2pq.files.paths.get_pq_file", lambda **kwargs: pq_file)
    monkeypatch.setattr("db2pq.files.parquet.get_modified_pq", lambda file_name: "local table (Updated 2026-03-28)")
    monkeypatch.setattr(update_mod, "pq_to_pg", lambda **kwargs: calls.append(kwargs) or True)

    assert pq_update_pg("example", "public", dbname="research") is True
    assert calls[0]["table_name"] == "example"
    assert calls[0]["schema"] == "public"
    assert calls[0]["source_comment"] == "local table (Updated 2026-03-28)"
    assert calls[0]["dbname"] == "research"


def test_pq_update_pg_messages_when_metadata_missing(monkeypatch, tmp_path, capsys):
    import db2pq.postgres.update as update_mod

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    pq_file = tmp_path / "public" / "example.parquet"
    pq_file.parent.mkdir(parents=True, exist_ok=True)
    pq_file.touch()

    called = False

    monkeypatch.setattr(update_mod, "resolve_uri", lambda **kwargs: "postgresql://dst")
    monkeypatch.setattr(update_mod, "get_pg_conn", lambda uri: DummyConn())
    monkeypatch.setattr("db2pq.files.paths.get_pq_file", lambda **kwargs: pq_file)
    monkeypatch.setattr("db2pq.files.parquet.get_modified_pq", lambda file_name: "")

    def fake_pq_to_pg(**kwargs):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(update_mod, "pq_to_pg", fake_pq_to_pg)

    assert pq_update_pg("example", "public", dbname="research") is False
    assert called is False
    out = capsys.readouterr().out
    assert "source parquet file has no parseable last_modified metadata" in out
    assert "force=True" in out


def test_pq_to_pg_adbc_uses_ingest(monkeypatch, tmp_path):
    import db2pq.postgres.update as update_mod

    class DummyPgConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyCursor:
        def __init__(self, calls):
            self.calls = calls

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def adbc_ingest(self, table_name, data, mode="create", db_schema_name=None):
            self.calls.append(
                {
                    "table_name": table_name,
                    "data": data,
                    "mode": mode,
                    "db_schema_name": db_schema_name,
                }
            )
            return 2

    class DummyAdbcConn:
        def __init__(self, calls):
            self.calls = calls
            self.committed = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return DummyCursor(self.calls)

        def commit(self):
            self.committed = True

    pq_file = tmp_path / "public" / "example.parquet"
    pq_file.parent.mkdir(parents=True, exist_ok=True)
    pq_file.touch()

    ingest_calls = []
    adbc_conn = DummyAdbcConn(ingest_calls)

    monkeypatch.setattr(update_mod, "get_pg_conn", lambda uri: DummyPgConn())
    monkeypatch.setattr(update_mod, "_ensure_schema_and_roles", lambda conn, schema, create_roles=True: None)
    monkeypatch.setattr(update_mod, "set_table_comment", lambda *args, **kwargs: None)
    monkeypatch.setattr(update_mod, "_apply_table_roles", lambda conn, schema, table_name: None)
    monkeypatch.setattr(update_mod, "_parquet_reader", lambda path: "reader")
    monkeypatch.setattr("adbc_driver_postgresql.dbapi.connect", lambda uri: adbc_conn)

    result = update_mod.parquet_write_pg(
        pq_file=pq_file,
        dst_uri="postgresql://dst",
        dst_schema="public",
        dst_table_name="example",
        engine="adbc",
        create_roles=False,
    )

    assert result is True
    assert ingest_calls == [
        {
            "table_name": "example",
            "data": "reader",
            "mode": "replace",
            "db_schema_name": "public",
        }
    ]
    assert adbc_conn.committed is True


def test_duckdb_load_parquet_to_pg_uses_precreate_and_insert(monkeypatch, tmp_path):
    import db2pq.postgres.update as update_mod

    pq_file = tmp_path / "public" / "example.parquet"
    pq_file.parent.mkdir(parents=True, exist_ok=True)
    pq_file.touch()

    statements = []

    class DummyDuckDBConn:
        def install_extension(self, name):
            statements.append(("install_extension", name, None))

        def load_extension(self, name):
            statements.append(("load_extension", name, None))

        def execute(self, sql, params=None):
            statements.append(("execute", sql, params))

        def close(self):
            statements.append(("close", None, None))

    monkeypatch.setattr("duckdb.connect", lambda: DummyDuckDBConn())

    update_mod._duckdb_load_parquet_to_pg(
        pq_file=pq_file,
        dst_uri="postgresql://dst",
        dst_schema="public",
        dst_table_name="example",
    )

    sqls = [entry[1] for entry in statements if entry[0] == "execute"]
    assert any("ATTACH 'postgresql://dst' AS pg" in sql for sql in sqls)
    assert any('DROP TABLE IF EXISTS pg."public"."example"' in sql for sql in sqls)
    assert any(
        'CREATE TABLE pg."public"."example" AS SELECT * FROM read_parquet(?) LIMIT 0' in sql
        for sql in sqls
    )
    assert any(
        'INSERT INTO pg."public"."example" SELECT * FROM read_parquet(?)' in sql
        for sql in sqls
    )


def test_count_wrds_rows_honors_where_and_obs(monkeypatch):
    import db2pq.postgres.select_sql as select_sql_mod

    seen = {}

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            seen["sql"] = sql

        def fetchone(self):
            return (25,)

    class DummyConn:
        def cursor(self):
            return DummyCursor()

    monkeypatch.setattr(select_sql_mod, "qident", lambda conn, name: f'"{name}"')

    result = select_sql_mod.count_wrds_rows(
        DummyConn(),
        schema="crsp",
        table="dsi",
        where="permno > 10000",
        obs=10,
    )

    assert result == 10
    assert seen["sql"] == 'SELECT COUNT(*) FROM "crsp"."dsi" WHERE permno > 10000'


def test_db_to_pq_duckdb_passes_total_rows_to_writer(monkeypatch, tmp_path):
    import db2pq.core as core_mod

    class DummyArrowQuery:
        total_rows = 1234
        progress_label = "crsp.dsi"

    seen = {}

    monkeypatch.setattr(
        "db2pq.postgres._defaults.resolve_pg_connection",
        lambda **kwargs: ("alice", "localhost", "research", 5432),
    )
    monkeypatch.setattr(
        "db2pq.postgres.duckdb_pg.read_postgres_table",
        lambda **kwargs: DummyArrowQuery(),
    )
    monkeypatch.setattr(
        "db2pq.files.parquet.write_parquet",
        lambda df, **kwargs: seen.update({"df": df, **kwargs}) or (tmp_path / "crsp" / "dsi.parquet"),
    )

    result = core_mod.db_to_pq(
        table_name="dsi",
        schema="crsp",
        user="alice",
        host="localhost",
        database="research",
        port=5432,
        data_dir=tmp_path,
        engine="duckdb",
    )

    assert result == str(tmp_path / "crsp" / "dsi.parquet")
    assert seen["total_rows"] == 1234
    assert seen["progress_label"] == "crsp.dsi"


def test_duckdb_arrow_query_uses_smaller_reader_batches():
    from db2pq.postgres.duckdb_pg import DuckDBArrowQuery

    seen = {}

    class DummyRelation:
        def fetch_arrow_reader(self, batch_size=1000000):
            seen["batch_size"] = batch_size
            return "reader"

    query = DuckDBArrowQuery(connection=object(), relation=DummyRelation())
    result = query.fetch_arrow_reader()

    assert result == "reader"
    assert seen["batch_size"] == 100_000


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


def test_pg_update_pq_writes_local_table_to_parquet(pg_test_config, src_pg_conn, data_dir):
    schema = "public"
    table = f"pg_update_pq_test_{uuid.uuid4().hex[:8]}"
    comment = "local table (Updated 2026-03-28)"

    try:
        with src_pg_conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{schema}"."{table}" (id integer, name text)')
            cur.execute(f'INSERT INTO "{schema}"."{table}" VALUES (1, %s), (2, %s)', ("a", "b"))
            cur.execute(f'COMMENT ON TABLE "{schema}"."{table}" IS %s', (comment,))
        src_pg_conn.commit()

        pq_file = pg_update_pq(
            table_name=table,
            schema=schema,
            user=pg_test_config["src_user"],
            host=pg_test_config["src_host"],
            database=pg_test_config["src_db"],
            port=pg_test_config["src_port"],
            data_dir=data_dir,
            engine="duckdb",
        )

        assert pq_file is not None
        assert pq.read_metadata(pq_file).num_rows == 2
        assert get_modified_pq(pq_file) == comment

        assert pg_update_pq(
            table_name=table,
            schema=schema,
            user=pg_test_config["src_user"],
            host=pg_test_config["src_host"],
            database=pg_test_config["src_db"],
            port=pg_test_config["src_port"],
            data_dir=data_dir,
            engine="duckdb",
        ) is None
    finally:
        with src_pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        src_pg_conn.commit()


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
