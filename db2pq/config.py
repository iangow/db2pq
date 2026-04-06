from __future__ import annotations

import os


_SESSION_DEFAULT_ENGINE: str | None = None
_VALID_ENGINES = {"duckdb", "adbc"}


def _normalize_engine(engine: str) -> str:
    value = engine.strip().lower()
    if value not in _VALID_ENGINES:
        raise ValueError("engine must be either 'duckdb' or 'adbc'")
    return value


def set_default_engine(engine: str) -> None:
    """
    Set the session-wide default engine used by db2pq helpers to create
    Parquet files from PostgreSQL data.

    Parameters
    ----------
    engine : {"duckdb", "adbc"}
        Default engine to use when a helper accepts ``engine=None``.
        The value is normalized to lowercase and stored for the current
        Python session only. ``"duckdb"`` is the package default and is the
        more established path. ``"adbc"`` is available for direct Arrow-based
        exports but should still be treated as a more experimental option.

    Examples
    ----------
    >>> from db2pq import db_to_pq, set_default_engine
    >>> set_default_engine("adbc")
    >>> db_to_pq("dsi", "crsp")
    >>> set_default_engine("duckdb")
    """
    global _SESSION_DEFAULT_ENGINE
    _SESSION_DEFAULT_ENGINE = _normalize_engine(engine)


def get_default_engine() -> str:
    """
    Return the default engine used by db2pq helpers.

    This function checks, in order, a session override set with
    ``set_default_engine()``, the ``DB2PQ_ENGINE`` environment variable,
    including values loaded from a local ``.env`` file when
    ``python-dotenv`` is installed, and finally the built-in default
    ``"duckdb"``.

    Returns
    -------
    str
        The current session override if one was set with
        ``set_default_engine()``. Otherwise, the value of the
        ``DB2PQ_ENGINE`` environment variable, including a value loaded from
        a local ``.env`` file if available. If neither is set, returns
        ``"duckdb"``.

    Examples
    ----------
    >>> from db2pq import get_default_engine
    >>> get_default_engine()

    >>> import os
    >>> os.environ["DB2PQ_ENGINE"] = "adbc"
    >>> get_default_engine()
    'adbc'
    """
    from .postgres.wrds import _load_dotenv

    if _SESSION_DEFAULT_ENGINE is not None:
        return _SESSION_DEFAULT_ENGINE

    _load_dotenv()

    env_value = os.getenv("DB2PQ_ENGINE")
    if env_value:
        return _normalize_engine(env_value)

    return "duckdb"
