from __future__ import annotations


def test_wrds_pg_to_pq_bootstraps_wrds_access(monkeypatch):
    import db2pq.core as core

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    captured = {}

    monkeypatch.setattr(core, "db_to_pq", lambda *args, **kwargs: kwargs)
    monkeypatch.setattr("db2pq.credentials.ensure_wrds_access", lambda wrds_id=None: "alice")
    monkeypatch.setattr("db2pq.postgres.comments.get_pg_conn", lambda uri: DummyConn())
    monkeypatch.setattr("db2pq.postgres.introspect.table_exists", lambda conn, schema, table: True)

    result = core.wrds_pg_to_pq("dsi", "crsp")

    assert result["user"] == "alice"
    assert result["host"] == "wrds-pgdata.wharton.upenn.edu"
    assert result["database"] == "wrds"
    assert result["port"] == 9737


def test_wrds_pg_to_pq_returns_none_when_source_table_missing(monkeypatch, capsys):
    import db2pq.core as core

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("db2pq.credentials.ensure_wrds_access", lambda wrds_id=None: "alice")
    monkeypatch.setattr("db2pq.postgres.comments.get_pg_conn", lambda uri: DummyConn())
    monkeypatch.setattr("db2pq.postgres.introspect.table_exists", lambda conn, schema, table: False)
    monkeypatch.setattr(core, "db_to_pq", lambda *args, **kwargs: "unexpected")

    assert core.wrds_pg_to_pq("mcti_corr", "crsp") is None
    out = capsys.readouterr().out
    assert "Table with name mcti_corr does not exist." in out
