from __future__ import annotations

import pytest

from db2pq.core import _resolve_numeric_mode
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


@pytest.mark.parametrize(
    ("engine", "numeric_mode", "expected"),
    [
        ("duckdb", None, None),
        ("adbc", None, "text"),
        ("duckdb", "decimal", "decimal"),
        ("duckdb", "text", "text"),
        ("adbc", "float64", "float64"),
    ],
)
def test_resolve_numeric_mode_defaults_and_overrides(engine, numeric_mode, expected):
    assert _resolve_numeric_mode(engine, numeric_mode) == expected


def test_resolve_numeric_mode_rejects_invalid_value():
    with pytest.raises(ValueError, match="numeric_mode must be one of"):
        _resolve_numeric_mode("duckdb", "bogus")
