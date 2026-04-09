from __future__ import annotations

import getpass
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg.conninfo import conninfo_to_dict

from .postgres.wrds import _load_dotenv, get_wrds_uri, resolve_wrds_id


@dataclass(frozen=True)
class CredentialTarget:
    hostname: str
    port: str
    database: str
    username: str
    passfile: Path


@dataclass(frozen=True)
class PgPassLookup:
    target: CredentialTarget
    entry: Any | None

    @property
    def found(self) -> bool:
        return self.entry is not None

    @property
    def password(self) -> str | None:
        if self.entry is None:
            return None
        return self.entry.password


def _default_pgpass_path() -> Path:
    if sys.platform == "win32":
        appdata = os.getenv("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(appdata) / "postgresql" / "pgpass.conf"
    return Path.home() / ".pgpass"


def _load_pgpass_tools():
    try:
        from pgtoolkit.pgpass import PassEntry, edit, parse
    except ImportError as exc:  # pragma: no cover - exercised when dependency missing
        raise ImportError(
            "pgtoolkit is required for db2pq.credentials helpers. "
            "Install db2pq with the package dependencies or `pip install pgtoolkit`."
        ) from exc
    return PassEntry, edit, parse


def _single_value(name: str, value: str | None) -> str | None:
    if value is None:
        return None
    if "," in value:
        raise ValueError(
            f"{name}={value!r} contains multiple values. "
            "db2pq.credentials only supports a single connection target."
        )
    return value


def _normalize_hostname(params: dict[str, str]) -> str:
    host = _single_value("host", params.get("host"))
    hostaddr = _single_value("hostaddr", params.get("hostaddr"))
    hostname = host or hostaddr or os.getenv("PGHOST") or "localhost"
    if hostname in {"/tmp", "/var/run/postgresql"}:
        return "localhost"
    return hostname


def _normalize_port(params: dict[str, str]) -> str:
    return str(_single_value("port", params.get("port")) or os.getenv("PGPORT") or "5432")


def _normalize_username(params: dict[str, str]) -> str:
    return _single_value("user", params.get("user")) or os.getenv("PGUSER") or getpass.getuser()


def _normalize_database(params: dict[str, str], username: str) -> str:
    return _single_value("dbname", params.get("dbname")) or os.getenv("PGDATABASE") or username


def _resolve_passfile(params: dict[str, str]) -> Path:
    raw_path = _single_value("passfile", params.get("passfile")) or os.getenv("PGPASSFILE")
    return Path(os.path.expanduser(raw_path)) if raw_path else _default_pgpass_path()


def _ensure_pgpass_permissions(path: Path) -> None:
    if sys.platform == "win32" or not path.exists():
        return
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise PermissionError(
            f"{path} has insecure permissions {oct(mode)}. "
            "PostgreSQL ignores .pgpass files unless they are accessible only by the owner."
        )


def _entry_field_matches(field: Any, value: str) -> bool:
    return str(field) == "*" or str(field) == value


def _entry_matches_target(entry: Any, target: CredentialTarget) -> bool:
    return all(
        [
            _entry_field_matches(entry.hostname, target.hostname),
            _entry_field_matches(entry.port, target.port),
            _entry_field_matches(entry.database, target.database),
            _entry_field_matches(entry.username, target.username),
        ]
    )


def _port_for_entry(port: str) -> int:
    return int(port)


def resolve_connection_target(conninfo: str = "", **kwargs: str | None) -> CredentialTarget:
    _load_dotenv()
    params = conninfo_to_dict(conninfo, **kwargs)
    username = _normalize_username(params)
    return CredentialTarget(
        hostname=_normalize_hostname(params),
        port=_normalize_port(params),
        database=_normalize_database(params, username),
        username=username,
        passfile=_resolve_passfile(params),
    )


def get_wrds_username(wrds_id: str | None = None) -> str:
    return resolve_wrds_id(wrds_id)


def get_wrds_conninfo(username: str | None = None) -> str:
    return get_wrds_uri(username)


def get_wrds_password() -> str | None:
    _load_dotenv()
    return os.getenv("WRDS_PASSWORD")


def find_pgpass_entry(conninfo: str = "", **kwargs: str | None) -> PgPassLookup:
    target = resolve_connection_target(conninfo, **kwargs)
    if not target.passfile.exists():
        return PgPassLookup(target=target, entry=None)

    _ensure_pgpass_permissions(target.passfile)
    _, _, parse = _load_pgpass_tools()
    passfile = parse(target.passfile)
    for entry in passfile:
        if _entry_matches_target(entry, target):
            return PgPassLookup(target=target, entry=entry)
    return PgPassLookup(target=target, entry=None)


def has_pgpass_password(conninfo: str = "", **kwargs: str | None) -> bool:
    return find_pgpass_entry(conninfo, **kwargs).found


def prompt_for_password(prompt: str | None = None) -> str:
    return getpass.getpass(prompt or "Enter PostgreSQL password: ")


def prompt_for_wrds_username(prompt: str | None = None) -> str:
    username = input(prompt or "Enter your WRDS username: ").strip()
    if not username:
        raise ValueError("WRDS username is required to connect to WRDS.")
    return username


def prompt_yes_no(prompt: str, *, default: bool = True) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    response = input(f"{prompt} {suffix} ").strip().lower()
    if not response:
        return default
    if response in {"y", "yes"}:
        return True
    if response in {"n", "no"}:
        return False
    raise ValueError("Please answer yes or no.")


def ensure_wrds_id(
    wrds_id: str | None = None,
    *,
    interactive: bool = True,
) -> str:
    try:
        return get_wrds_username(wrds_id)
    except ValueError:
        if not interactive:
            raise
        username = prompt_for_wrds_username(
            "WRDS username not found. Enter your WRDS username: "
        )
        print(
            "Tip: add "
            f"`WRDS_ID={username}` "
            "to a local `.env` file in the calling project so you only have to do this once."
        )
        return username


def ensure_wrds_access(
    wrds_id: str | None = None,
    *,
    interactive: bool = True,
) -> str:
    username = ensure_wrds_id(wrds_id, interactive=interactive)
    conninfo = get_wrds_conninfo(username)
    if not has_pgpass_password(conninfo):
        env_password = get_wrds_password()
        if env_password and interactive:
            passfile = resolve_connection_target(conninfo).passfile
            if prompt_yes_no(
                f"Found `WRDS_PASSWORD` in the environment. Save it to {passfile} now?",
                default=True,
            ):
                save_password(conninfo, password=env_password)
                print(
                    f"Saved WRDS PostgreSQL credentials to {passfile}. "
                    "You can remove `WRDS_PASSWORD` from your environment once this is working."
                )
                return username
        if not interactive:
            raise ValueError(
                "No WRDS password found in .pgpass for the resolved WRDS connection."
            )
        password = prompt_for_password("Enter your WRDS PostgreSQL password: ")
        save_password(conninfo, password=password)
        print(f"Saved WRDS PostgreSQL credentials to {resolve_connection_target(conninfo).passfile}.")
    return username


def _probe_connection(target: CredentialTarget) -> None:
    import psycopg

    conninfo = (
        f"postgresql://{target.username}@{target.hostname}:{target.port}/{target.database}"
    )
    with psycopg.connect(conninfo, passfile=str(target.passfile)):
        return


def _is_auth_failure(exc: Exception) -> bool:
    message = str(exc).lower()
    auth_markers = (
        "password authentication failed",
        "no password supplied",
        "fe_sendauth",
        "pg_hba.conf",
    )
    return any(marker in message for marker in auth_markers)


def ensure_pg_access(
    conninfo: str = "",
    *,
    interactive: bool = True,
    password_env_var: str = "PGPASSWORD",
    **kwargs: str | None,
) -> CredentialTarget:
    target = resolve_connection_target(conninfo, **kwargs)

    try:
        _probe_connection(target)
        return target
    except Exception as exc:
        if not _is_auth_failure(exc):
            raise

    env_password = os.getenv(password_env_var) if password_env_var else None
    if env_password and interactive:
        if prompt_yes_no(
            f"Found `{password_env_var}` in the environment. Save it to {target.passfile} now?",
            default=True,
        ):
            save_password(conninfo, password=env_password, **kwargs)
            print(
                f"Saved PostgreSQL credentials to {target.passfile}. "
                f"You can remove `{password_env_var}` from your environment once this is working."
            )
            return target

    if not interactive:
        raise ValueError(
            "No PostgreSQL password found in .pgpass for the resolved connection."
        )

    password = prompt_for_password(
        f"Enter PostgreSQL password for {target.username}@{target.hostname}:{target.port}/{target.database}: "
    )
    save_password(conninfo, password=password, **kwargs)
    print(f"Saved PostgreSQL credentials to {target.passfile}.")
    return target


def save_password(
    conninfo: str = "",
    password: str | None = None,
    *,
    prompt: str | None = None,
    **kwargs: str | None,
) -> Path:
    target = resolve_connection_target(conninfo, **kwargs)
    if target.passfile.exists():
        _ensure_pgpass_permissions(target.passfile)
    else:
        target.passfile.parent.mkdir(parents=True, exist_ok=True)

    if password is None:
        prompt = prompt or (
            f"Enter PostgreSQL password for "
            f"{target.username}@{target.hostname}:{target.port}/{target.database}: "
        )
        password = prompt_for_password(prompt)

    if not password:
        raise ValueError("password must not be empty")

    PassEntry, edit, _ = _load_pgpass_tools()
    with edit(target.passfile) as passfile:
        passfile.remove(
            hostname=target.hostname,
            port=_port_for_entry(target.port),
            database=target.database,
            username=target.username,
        )
        passfile.lines.append(
            PassEntry(
                target.hostname,
                _port_for_entry(target.port),
                target.database,
                target.username,
                password,
            )
        )
        passfile.sort()

    if sys.platform != "win32":
        os.chmod(target.passfile, stat.S_IRUSR | stat.S_IWUSR)
    return target.passfile


def ensure_wrds_credentials(
    wrds_id: str | None = None,
    *,
    interactive: bool = True,
) -> bool:
    ensure_wrds_access(wrds_id, interactive=interactive)
    return True


__all__ = [
    "CredentialTarget",
    "ensure_pg_access",
    "PgPassLookup",
    "ensure_wrds_access",
    "ensure_wrds_credentials",
    "ensure_wrds_id",
    "find_pgpass_entry",
    "get_wrds_conninfo",
    "get_wrds_password",
    "get_wrds_username",
    "has_pgpass_password",
    "prompt_for_password",
    "prompt_for_wrds_username",
    "prompt_yes_no",
    "resolve_connection_target",
    "save_password",
]
