import os

from dotenv import load_dotenv

WRDS_PUBLIC_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PRIVATE_HOST = "wrds-pgdata-ident-w.wharton.private"
WRDS_PORT = 9737
WRDS_DB   = "wrds"
_USE_PRIVATE_DEFAULT = False


def set_wrds_use_private(use_private: bool) -> None:
    global _USE_PRIVATE_DEFAULT
    _USE_PRIVATE_DEFAULT = bool(use_private)


def get_wrds_use_private() -> bool:
    return _USE_PRIVATE_DEFAULT


def resolve_wrds_use_private(use_private: bool | None = None) -> bool:
    load_dotenv()
    if use_private is not None:
        return bool(use_private)

    env_value = os.getenv("DB2PQ_WRDS_USE_PRIVATE")
    if env_value is not None:
        return env_value.strip().lower() in {"1", "true", "yes", "on"}

    return _USE_PRIVATE_DEFAULT


def resolve_wrds_host(use_private: bool | None = None) -> str:
    if resolve_wrds_use_private(use_private):
        return WRDS_PRIVATE_HOST
    return WRDS_PUBLIC_HOST

def resolve_wrds_id(wrds_id: str | None = None) -> str:
    load_dotenv()
    wrds_id = wrds_id or os.getenv("WRDS_ID")
    if not wrds_id:
        raise ValueError(
            "wrds_id must be provided either as an argument or "
            "via the WRDS_ID environment variable"
        )
    return wrds_id

def get_wrds_uri(wrds_id: str | None = None, *, use_private: bool | None = None) -> str:
    """Return a PostgreSQL connection URI for the WRDS database."""
    wrds_id = resolve_wrds_id(wrds_id)
    wrds_host = resolve_wrds_host(use_private)
    return f"postgresql://{wrds_id}@{wrds_host}:{WRDS_PORT}/{WRDS_DB}"
