import os

WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT = 9737
WRDS_DB   = "wrds"

def _load_dotenv() -> None:
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        return
    dotenv_path = find_dotenv(usecwd=True)
    load_dotenv(dotenv_path or None)

def resolve_wrds_id(wrds_id: str | None = None) -> str:
    _load_dotenv()
    wrds_id = wrds_id or os.getenv("WRDS_ID") or os.getenv("WRDS_USER")
    if not wrds_id:
        raise ValueError(
            "WRDS username not found.\n"
            "Provide `wrds_id=...` or set `WRDS_ID` in your environment.\n"
            "For compatibility with Tidy Finance-style setups, `WRDS_USER` is also recognized.\n"
            "For example, add `WRDS_ID=your_wrds_id` to a local `.env` file "
            "in the calling project."
        )
    return wrds_id

def get_wrds_uri(wrds_id: str | None = None) -> str:
    """Return a PostgreSQL connection URI for the WRDS database."""
    wrds_id = resolve_wrds_id(wrds_id)
    return f"postgresql://{wrds_id}@{WRDS_HOST}:{WRDS_PORT}/{WRDS_DB}"
