from __future__ import annotations

from types import SimpleNamespace

import pytest

import db2pq.ibis as ibis_mod


class _FakeTable:
    def __init__(self, *, sql: str = "SELECT 1", backend_name: str = "postgres"):
        info = SimpleNamespace(
            user="alice",
            password="secret",
            host="localhost",
            port=5432,
            dbname="research",
        )
        self._backend = SimpleNamespace(
            name=backend_name,
            con=SimpleNamespace(info=info),
        )
        self._sql = sql

    def get_backend(self):
        return self._backend

    def compile(self):
        return self._sql


class _FakeArrowQuery:
    def __init__(self):
        self.reader = object()

    def fetch_arrow_reader(self):
        return self.reader


def test_ibis_to_pq_duckdb_uses_default_engine(monkeypatch, tmp_path):
    table = _FakeTable(sql='SELECT * FROM "public"."example"')
    calls = {}

    def fake_read_postgres_query(**kwargs):
        calls["query"] = kwargs
        return _FakeArrowQuery()

    def fake_write_record_batch_reader_to_parquet(reader, out_file, **kwargs):
        calls["write"] = {"reader": reader, "out_file": out_file, **kwargs}
        return True

    monkeypatch.setattr("db2pq.postgres.duckdb_pg.read_postgres_query", fake_read_postgres_query)
    monkeypatch.setattr(
        "db2pq.files.parquet.write_record_batch_reader_to_parquet",
        fake_write_record_batch_reader_to_parquet,
    )
    monkeypatch.setattr("db2pq.config.get_default_engine", lambda: "duckdb")

    out_file = tmp_path / "example.parquet"
    result = ibis_mod.ibis_to_pq(
        table,
        out_file,
        compression="zstd",
        threads=3,
    )

    assert result == str(out_file)
    assert calls["query"]["sql"] == 'SELECT * FROM "public"."example"'
    assert calls["query"]["threads"] == 3
    assert calls["write"]["out_file"] == out_file
    assert calls["write"]["parquet_writer_kwargs"] == {"compression": "zstd"}


def test_ibis_to_pq_adbc_dispatches_to_shared_export(monkeypatch, tmp_path):
    table = _FakeTable(sql='SELECT * FROM "public"."example" WHERE id > 10')
    calls = {}

    def fake_export_postgres_query_via_adbc(**kwargs):
        calls["adbc"] = kwargs
        return str(kwargs["out_file"])

    monkeypatch.setattr(
        "db2pq.postgres.adbc.export_postgres_query_via_adbc",
        fake_export_postgres_query_via_adbc,
    )

    out_file = tmp_path / "example.parquet"
    result = ibis_mod.ibis_to_pq(
        table,
        out_file,
        engine="adbc",
        compression="zstd",
        adbc_use_copy=True,
        adbc_batch_size_hint_bytes=1024,
    )

    assert result == str(out_file)
    assert calls["adbc"]["sql"] == 'SELECT * FROM "public"."example" WHERE id > 10'
    assert calls["adbc"]["adbc_use_copy"] is True
    assert calls["adbc"]["adbc_batch_size_hint_bytes"] == 1024
    assert calls["adbc"]["parquet_writer_kwargs"] == {"compression": "zstd"}


def test_ibis_to_pq_rejects_non_postgres_backend(tmp_path):
    table = _FakeTable(backend_name="duckdb")

    with pytest.raises(TypeError, match="PostgreSQL-backed"):
        ibis_mod.ibis_to_pq(table, tmp_path / "example.parquet")


def test_ibis_to_pq_rejects_unknown_engine(tmp_path):
    table = _FakeTable()

    with pytest.raises(ValueError, match="engine must be either 'duckdb' or 'adbc'"):
        ibis_mod.ibis_to_pq(table, tmp_path / "example.parquet", engine="sqlite")


def test_backend_to_uri_handles_missing_password():
    table = _FakeTable()
    table._backend.con.info.password = None

    assert ibis_mod._backend_to_uri(table.get_backend()) == "postgresql://alice@localhost:5432/research"
