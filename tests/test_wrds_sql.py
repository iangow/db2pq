from __future__ import annotations

import db2pq.core as core_mod


class _FakeArrowQuery:
    def __init__(self):
        self.reader = object()
        self.total_rows = None

    def fetch_arrow_reader(self):
        return self.reader


def test_wrds_sql_to_pq_duckdb_uses_repo_layout(monkeypatch, tmp_path):
    calls = {}

    def fake_read_postgres_query(**kwargs):
        calls["query"] = kwargs
        return _FakeArrowQuery()

    def fake_write_record_batch_reader_to_parquet(reader, out_file, **kwargs):
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_bytes(b"PAR1")
        calls["write"] = {"reader": reader, "out_file": out_file, **kwargs}
        return True

    monkeypatch.setattr("db2pq.credentials.ensure_wrds_access", lambda wrds_id=None: "alice")
    monkeypatch.setattr("db2pq.config.get_default_engine", lambda: "duckdb")
    monkeypatch.setattr("db2pq.postgres.duckdb_pg.read_postgres_query", fake_read_postgres_query)
    monkeypatch.setattr(
        "db2pq.files.parquet.write_record_batch_reader_to_parquet",
        fake_write_record_batch_reader_to_parquet,
    )

    result = core_mod.wrds_sql_to_pq(
        sql="SELECT * FROM comp.funda WHERE fyear >= 2000",
        table_name="funda",
        schema="comp",
        data_dir=tmp_path,
        alt_table_name="funda_filtered",
        threads=4,
    )

    expected = tmp_path / "comp" / "funda_filtered.parquet"
    expected_tmp = tmp_path / "comp" / ".temp_funda_filtered.parquet"

    assert result == str(expected)
    assert calls["query"]["uri"] == "postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds"
    assert calls["query"]["sql"] == "SELECT * FROM comp.funda WHERE fyear >= 2000"
    assert calls["query"]["threads"] == 4
    assert calls["write"]["out_file"] == expected_tmp
    assert calls["write"]["progress_label"] == "comp.funda_filtered"


def test_wrds_sql_to_pq_adbc_dispatches_to_shared_export(monkeypatch, tmp_path):
    calls = {}

    def fake_export_postgres_query_via_adbc(**kwargs):
        calls["adbc"] = kwargs
        out_file = kwargs["out_file"]
        out_file.write_bytes(b"PAR1")
        return str(out_file)

    monkeypatch.setattr("db2pq.credentials.ensure_wrds_access", lambda wrds_id=None: "alice")
    monkeypatch.setattr("db2pq.config.get_default_engine", lambda: "adbc")
    monkeypatch.setattr(
        "db2pq.postgres.adbc.export_postgres_query_via_adbc",
        fake_export_postgres_query_via_adbc,
    )

    result = core_mod.wrds_sql_to_pq(
        sql="SELECT gvkey FROM comp.funda",
        table_name="funda",
        schema="comp",
        data_dir=tmp_path,
        adbc_use_copy=True,
        adbc_batch_size_hint_bytes=2048,
    )

    expected = tmp_path / "comp" / "funda.parquet"
    expected_tmp = tmp_path / "comp" / ".temp_funda.parquet"

    assert result == str(expected)
    assert calls["adbc"]["uri"] == "postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds"
    assert calls["adbc"]["sql"] == "SELECT gvkey FROM comp.funda"
    assert calls["adbc"]["out_file"] == expected_tmp
    assert calls["adbc"]["adbc_use_copy"] is True
    assert calls["adbc"]["adbc_batch_size_hint_bytes"] == 2048
