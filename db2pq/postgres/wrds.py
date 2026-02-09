import os

def resolve_wrds_id(wrds_id=None) -> str:
    wrds_id = wrds_id or os.getenv("WRDS_ID")

    if not wrds_id:
        raise ValueError("Provide wrds_id or set WRDS_ID")
    return wrds_id

def get_wrds_uri(wrds_id: str) -> str:
    # adjust sslmode if needed; WRDS often works with defaults
    return f"postgresql://{wrds_id}@wrds-pgdata.wharton.upenn.edu:9737/wrds"
