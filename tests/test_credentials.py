from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys

import pytest

from db2pq.credentials import (
    ensure_wrds_access,
    ensure_wrds_credentials,
    ensure_wrds_id,
    find_pgpass_entry,
    has_pgpass_password,
    resolve_connection_target,
    save_password,
)


def _install_fake_pgtoolkit(monkeypatch):
    import db2pq.credentials as credentials

    class FakePassEntry:
        def __init__(self, hostname, port, database, username, password):
            self.hostname = hostname
            self.port = port
            self.database = database
            self.username = username
            self.password = password

        def sort_key(self):
            fields = [self.hostname, str(self.port), self.database, self.username]
            precision = sum(field == "*" for field in fields)
            sortable = [chr(0xFF) if field == "*" else field for field in fields]
            return (precision, *sortable)

        def __lt__(self, other):
            return self.sort_key() < other.sort_key()

        def __str__(self):
            return ":".join(
                [
                    str(self.hostname).replace("\\", "\\\\").replace(":", "\\:"),
                    str(self.port),
                    str(self.database).replace("\\", "\\\\").replace(":", "\\:"),
                    str(self.username).replace("\\", "\\\\").replace(":", "\\:"),
                    str(self.password).replace("\\", "\\\\").replace(":", "\\:"),
                ]
            )

    class FakePassFile:
        def __init__(self, lines=None, *, path=None):
            self.lines = lines or []
            self.path = str(path) if path is not None else None

        def __iter__(self):
            return iter(self.lines)

        def remove(self, **attrs):
            self.lines = [
                line
                for line in self.lines
                if not all(getattr(line, key) == value for key, value in attrs.items())
            ]

        def sort(self):
            self.lines.sort()

        def save(self):
            assert self.path is not None
            Path(self.path).write_text(
                "\n".join(str(line) for line in self.lines) + ("\n" if self.lines else ""),
                encoding="utf-8",
            )

    def fake_parse(path):
        path = Path(path)
        entries = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line or line.startswith("#"):
                    continue
                host, port, database, username, password = line.split(":")
                port_value = "*" if port == "*" else int(port)
                entries.append(FakePassEntry(host, port_value, database, username, password))
        return FakePassFile(entries, path=path)

    @contextmanager
    def fake_edit(path):
        path = Path(path)
        passfile = fake_parse(path) if path.exists() else FakePassFile(path=path)
        yield passfile
        passfile.save()

    monkeypatch.setattr(
        credentials,
        "_load_pgpass_tools",
        lambda: (FakePassEntry, fake_edit, fake_parse),
    )


def test_resolve_connection_target_uses_env_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("PGUSER", "alice")
    monkeypatch.setenv("PGDATABASE", "research")
    monkeypatch.setenv("PGHOST", "db.example.com")
    monkeypatch.setenv("PGPORT", "6543")
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / ".pgpass"))

    target = resolve_connection_target("")

    assert target.username == "alice"
    assert target.database == "research"
    assert target.hostname == "db.example.com"
    assert target.port == "6543"
    assert target.passfile == tmp_path / ".pgpass"


def test_find_pgpass_entry_honors_first_match(monkeypatch, tmp_path):
    _install_fake_pgtoolkit(monkeypatch)
    pgpass = tmp_path / ".pgpass"
    pgpass.write_text(
        "*:9737:wrds:alice:wildcard\n"
        "wrds-pgdata.wharton.upenn.edu:9737:wrds:alice:exact\n",
        encoding="utf-8",
    )
    if sys.platform != "win32":
        pgpass.chmod(0o600)

    lookup = find_pgpass_entry(
        "postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds",
        passfile=str(pgpass),
    )

    assert lookup.found is True
    assert lookup.password == "wildcard"


def test_save_password_writes_entry(monkeypatch, tmp_path):
    _install_fake_pgtoolkit(monkeypatch)
    pgpass = tmp_path / ".pgpass"

    path = save_password(
        "postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds",
        password="secret",
        passfile=str(pgpass),
    )

    assert path == pgpass
    assert has_pgpass_password(
        "postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds",
        passfile=str(pgpass),
    )
    assert "secret" in pgpass.read_text(encoding="utf-8")


def test_ensure_wrds_credentials_prompts_and_saves(monkeypatch, tmp_path):
    _install_fake_pgtoolkit(monkeypatch)
    monkeypatch.setenv("WRDS_ID", "alice")
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / ".pgpass"))
    monkeypatch.setattr("db2pq.credentials.prompt_for_password", lambda prompt=None: "secret")

    assert ensure_wrds_credentials() is True
    assert has_pgpass_password("postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds")


def test_ensure_wrds_id_prompts_when_missing(monkeypatch, capsys):
    monkeypatch.delenv("WRDS_ID", raising=False)
    monkeypatch.setattr("builtins.input", lambda prompt="": "alice")

    username = ensure_wrds_id()

    assert username == "alice"
    assert "Tip: add `WRDS_ID=alice`" in capsys.readouterr().out


def test_ensure_wrds_access_prompts_for_username_and_password(monkeypatch, tmp_path):
    _install_fake_pgtoolkit(monkeypatch)
    monkeypatch.delenv("WRDS_ID", raising=False)
    monkeypatch.setenv("PGPASSFILE", str(tmp_path / ".pgpass"))
    monkeypatch.setattr("builtins.input", lambda prompt="": "alice")
    monkeypatch.setattr("db2pq.credentials.prompt_for_password", lambda prompt=None: "secret")

    username = ensure_wrds_access()

    assert username == "alice"
    assert has_pgpass_password("postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds")


def test_ensure_wrds_credentials_missing_wrds_id_has_friendly_error(monkeypatch):
    monkeypatch.delenv("WRDS_ID", raising=False)

    monkeypatch.setattr("db2pq.credentials.prompt_for_wrds_username", lambda prompt=None: (_ for _ in ()).throw(ValueError("WRDS username not found.")))

    with pytest.raises(ValueError, match="WRDS username not found"):
        ensure_wrds_credentials()


def test_find_pgpass_entry_rejects_insecure_permissions(monkeypatch, tmp_path):
    if sys.platform == "win32":
        pytest.skip("Unix-style pgpass permission checks do not apply on Windows")

    _install_fake_pgtoolkit(monkeypatch)
    pgpass = tmp_path / ".pgpass"
    pgpass.write_text(
        "wrds-pgdata.wharton.upenn.edu:9737:wrds:alice:secret\n",
        encoding="utf-8",
    )
    pgpass.chmod(0o644)

    with pytest.raises(PermissionError):
        find_pgpass_entry(
            "postgresql://alice@wrds-pgdata.wharton.upenn.edu:9737/wrds",
            passfile=str(pgpass),
        )
