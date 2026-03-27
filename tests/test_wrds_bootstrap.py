from __future__ import annotations


def test_wrds_pg_to_pq_bootstraps_wrds_access(monkeypatch):
    import db2pq.core as core

    captured = {}

    monkeypatch.setattr(core, "db_to_pq", lambda *args, **kwargs: kwargs)
    monkeypatch.setattr("db2pq.credentials.ensure_wrds_access", lambda wrds_id=None: "alice")

    result = core.wrds_pg_to_pq("dsi", "crsp")

    assert result["user"] == "alice"
    assert result["host"] == "wrds-pgdata.wharton.upenn.edu"
    assert result["database"] == "wrds"
    assert result["port"] == 9737
