import os

WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT = 9737
WRDS_DB   = "wrds"

def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()

def resolve_wrds_id(wrds_id: str | None = None) -> str:
    _load_dotenv()
    wrds_id = wrds_id or os.getenv("WRDS_ID")
    if not wrds_id:
        raise ValueError(
            "wrds_id must be provided either as an argument or "
            "via the WRDS_ID environment variable"
        )
    return wrds_id

def get_wrds_uri(wrds_id: str | None = None) -> str:
    """Return a PostgreSQL connection URI for the WRDS database."""
    wrds_id = resolve_wrds_id(wrds_id)
    return f"postgresql://{wrds_id}@{WRDS_HOST}:{WRDS_PORT}/{WRDS_DB}"
