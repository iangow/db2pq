import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from db2pq import pq_check, pq_inventory
from db2pq.__main__ import main
from db2pq.credentials import (
    PromptUnavailableError,
    ensure_wrds_access,
    ensure_wrds_id,
    is_interactive,
    prompt_for_wrds_username,
)


def _write(root, schema, table, n=3, modified=None):
    path = root / schema / f"{table}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    tbl = pa.table({"a": list(range(n)), "b": ["x"] * n})
    if modified:
        tbl = tbl.replace_schema_metadata({"last_modified": modified})
    pq.write_table(tbl, path)
    return path


def test_pq_inventory_reports_rows_columns_and_last_modified(tmp_path):
    _write(tmp_path, "crsp", "dsi", n=5, modified="Last modified: 2026-01-02")
    _write(tmp_path, "comp", "funda", n=2)
    (tmp_path / "crsp" / ".temp_dsf.parquet").write_bytes(b"")

    rows = pq_inventory(data_dir=tmp_path)

    assert [(r["schema"], r["table"]) for r in rows] == [("comp", "funda"), ("crsp", "dsi")]
    dsi = rows[1]
    assert (dsi["n_rows"], dsi["n_columns"]) == (5, 2)
    assert dsi["last_modified"] == "Last modified: 2026-01-02"
    assert rows[0]["last_modified"] is None
    assert pq_inventory("crsp", data_dir=tmp_path) == [dsi]


def test_pq_inventory_of_a_missing_directory_is_empty(tmp_path):
    assert pq_inventory(data_dir=tmp_path / "nope") == []


def test_pq_check_lists_missing_tables_and_creates_nothing(tmp_path):
    _write(tmp_path, "crsp", "dsi")

    result = pq_check(["crsp.dsi", ("funda", "comp")], data_dir=tmp_path)

    assert result["ok"] is False
    assert result["present"] == ["crsp.dsi"]
    assert result["missing"] == ["comp.funda"]
    assert not (tmp_path / "comp").exists()


def test_pq_check_flags_an_unreadable_file_as_missing(tmp_path):
    bad = tmp_path / "crsp" / "dsf.parquet"
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"not parquet")

    result = pq_check(["crsp.dsf"], data_dir=tmp_path)

    assert result["missing"] == ["crsp.dsf"]
    assert result["tables"][0]["error"]


def test_pq_check_rejects_a_name_without_schema(tmp_path):
    with pytest.raises(ValueError, match="schema.table"):
        pq_check(["dsf"], data_dir=tmp_path)


def test_cli_check_exit_code_and_json(tmp_path, capsys):
    _write(tmp_path, "crsp", "dsi")

    assert main(["check", "crsp.dsi", "--data-dir", str(tmp_path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert main(["check", "crsp.dsf", "--data-dir", str(tmp_path), "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["missing"] == ["crsp.dsf"]


def test_cli_status_json(tmp_path, capsys):
    _write(tmp_path, "crsp", "dsi", n=4)

    assert main(["status", "--data-dir", str(tmp_path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "argument"
    assert payload["tables"][0]["n_rows"] == 4


def test_cli_doctor_reports_cwd_fallback_and_never_prints_the_password(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setenv("WRDS_ID", "alice")
    monkeypatch.setenv("WRDS_PASSWORD", "s3cret-value")
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "pgpass"))

    assert main(["doctor", "--json"]) == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["source"] == "cwd"
    assert payload["wrds_id_set"] is True
    assert payload["wrds_password_env_set"] is True
    assert payload["wrds_pgpass_entry"] is False
    assert any("DATA_DIR" in p for p in payload["problems"])
    assert "s3cret-value" not in out


def test_noninteractive_env_disables_prompts(monkeypatch, tmp_path):
    monkeypatch.setenv("DB2PQ_NONINTERACTIVE", "1")
    monkeypatch.delenv("WRDS_ID", raising=False)
    monkeypatch.delenv("WRDS_USER", raising=False)
    monkeypatch.chdir(tmp_path)

    def fail(*args, **kwargs):
        raise AssertionError("prompted")

    monkeypatch.setattr("builtins.input", fail)
    monkeypatch.setattr("getpass.getpass", fail)

    assert is_interactive() is False
    assert is_interactive(True) is True
    with pytest.raises(ValueError, match="WRDS_ID"):
        ensure_wrds_id()

    monkeypatch.setenv("WRDS_ID", "alice")
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / "pgpass"))
    monkeypatch.delenv("WRDS_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="save-wrds-password"):
        ensure_wrds_access()


def test_closed_stdin_raises_a_named_error_instead_of_eoferror(monkeypatch):
    def eof(*args, **kwargs):
        raise EOFError

    monkeypatch.setattr("builtins.input", eof)
    with pytest.raises(PromptUnavailableError, match="WRDS_ID"):
        prompt_for_wrds_username()


def test_cli_save_wrds_password_writes_pgpass_without_a_prompt(
    monkeypatch, tmp_path, capsys
):
    passfile = tmp_path / "pgpass"
    monkeypatch.setenv("PGPASSFILE", str(passfile))
    monkeypatch.setenv("WRDS_ID", "alice")
    monkeypatch.setenv("WRDS_PASSWORD", "s3cret-value")
    monkeypatch.setattr("builtins.input", lambda *a, **k: pytest.fail("prompted"))

    assert main(["save-wrds-password", "--json"]) == 0
    out = capsys.readouterr().out
    assert "s3cret-value" not in out
    assert "alice" in passfile.read_text(encoding="utf-8")
    monkeypatch.delenv("WRDS_PASSWORD")
    monkeypatch.setenv("DB2PQ_NONINTERACTIVE", "1")
    assert ensure_wrds_access() == "alice"
