from __future__ import annotations


def test_db_to_pq_defaults_modified_from_table_comment(monkeypatch, tmp_path):
    import db2pq.core as core_mod

    seen = {}

    monkeypatch.setattr(
        "db2pq.postgres.comments.get_pg_comment",
        lambda **kwargs: "local table (Updated 2026-03-28)",
    )
    monkeypatch.setattr(
        "db2pq.postgres._defaults.resolve_pg_connection",
        lambda **kwargs: ("alice", "localhost", "research", 5432),
    )
    monkeypatch.setattr("db2pq.credentials.ensure_pg_access", lambda **kwargs: None)
    monkeypatch.setattr("db2pq.config.get_default_engine", lambda: "duckdb")
    monkeypatch.setattr(
        "db2pq.postgres.duckdb_pg.read_postgres_table",
        lambda **kwargs: seen.update(read_kwargs=kwargs) or object(),
    )
    monkeypatch.setattr(
        "db2pq.files.parquet.write_parquet",
        lambda df, **kwargs: seen.update(kwargs) or (tmp_path / "public" / "example.parquet"),
    )

    result = core_mod.db_to_pq(
        table_name="example",
        schema="public",
        user="alice",
        host="localhost",
        database="research",
        port=5432,
        data_dir=tmp_path,
        engine="duckdb",
        rename={"id": "example_id"},
    )

    assert result == str(tmp_path / "public" / "example.parquet")
    assert seen["modified"] == "local table (Updated 2026-03-28)"
    assert seen["read_kwargs"]["rename"] == {"id": "example_id"}


def test_pg_update_pq_relies_on_db_to_pq_default_metadata(monkeypatch, tmp_path):
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
    monkeypatch.setattr("db2pq.credentials.ensure_pg_access", lambda **kwargs: None)

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
        rename={"id": "example_id"},
    )

    assert result == str(tmp_path / "public" / "example.parquet")
    assert seen["table_name"] == "example"
    assert seen["schema"] == "public"
    assert "modified" not in seen
    assert seen["database"] == "research"
    assert seen["where"] == "id > 10"
    assert seen["rename"] == {"id": "example_id"}


def test_db_to_pg_forwards_rename(monkeypatch):
    import db2pq.core as core_mod

    seen = {}

    monkeypatch.setattr(
        "db2pq.postgres._defaults.resolve_pg_connection",
        lambda **kwargs: (
            kwargs.get("user") or "alice",
            kwargs.get("host") or "localhost",
            kwargs.get("dbname") or "research",
            kwargs.get("port") or 5432,
        ),
    )
    monkeypatch.setattr("db2pq.credentials.ensure_pg_access", lambda **kwargs: None)
    monkeypatch.setattr(
        "db2pq.postgres.update.postgres_write_pg",
        lambda **kwargs: seen.update(kwargs) or True,
    )

    result = core_mod.db_to_pg(
        table_name="example",
        schema="public",
        user="alice",
        host="localhost",
        database="research",
        port=5432,
        dst_user="bob",
        dst_host="localhost",
        dst_database="analytics",
        dst_port=5433,
        rename={"id": "example_id"},
    )

    assert result is True
    assert seen["rename"] == {"id": "example_id"}


def test_wrds_update_pq_uses_default_table_comment_metadata_when_not_using_sas(monkeypatch):
    import db2pq.core as core_mod
    import db2pq.credentials as credentials_mod

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    seen = {}

    monkeypatch.setattr(credentials_mod, "ensure_wrds_access", lambda wrds_id=None: "user")
    monkeypatch.setattr("db2pq.postgres.comments.get_wrds_conn", lambda wrds_id=None: DummyConn())
    monkeypatch.setattr("db2pq.postgres.introspect.table_exists", lambda conn, schema, table: True)
    monkeypatch.setattr(
        "db2pq.postgres.comments.get_wrds_comment",
        lambda **kwargs: "WRDS table comment (Updated 2026-04-01)",
    )
    monkeypatch.setattr(core_mod, "_update_pq", lambda **kwargs: seen.update(kwargs) or "ok")

    result = core_mod.wrds_update_pq("mcti_corr", "crsp", rename={"permno": "perm_id"})

    assert result == "ok"
    assert seen["source_comment"] == "WRDS table comment (Updated 2026-04-01)"
    assert "modified" not in seen
    assert seen["rename"] == {"permno": "perm_id"}


def test_wrds_update_pq_overrides_metadata_when_using_sas(monkeypatch):
    import db2pq.core as core_mod
    import db2pq.credentials as credentials_mod

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    seen = {}

    monkeypatch.setattr(credentials_mod, "ensure_wrds_access", lambda wrds_id=None: "user")
    monkeypatch.setattr("db2pq.postgres.comments.get_wrds_conn", lambda wrds_id=None: DummyConn())
    monkeypatch.setattr("db2pq.postgres.introspect.table_exists", lambda conn, schema, table: True)
    monkeypatch.setattr(
        "db2pq.postgres.comments.get_wrds_comment",
        lambda **kwargs: "Last modified: 2026-04-01",
    )
    monkeypatch.setattr(core_mod, "_update_pq", lambda **kwargs: seen.update(kwargs) or "ok")

    result = core_mod.wrds_update_pq("mcti_corr", "crsp", use_sas=True)

    assert result == "ok"
    assert seen["source_comment"] == "Last modified: 2026-04-01"
    assert seen["modified"] == "Last modified: 2026-04-01"
