from __future__ import annotations

from db2pq.postgres._defaults import resolve_pg_connection


def test_resolve_pg_connection_loads_dotenv(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "PGUSER=alice\nPGDATABASE=research\nPGHOST=db.example.com\nPGPORT=6543\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)

    assert resolve_pg_connection() == ("alice", "db.example.com", "research", 6543)
