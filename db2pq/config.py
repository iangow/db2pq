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
    global _SESSION_DEFAULT_ENGINE
    _SESSION_DEFAULT_ENGINE = _normalize_engine(engine)


def get_default_engine() -> str:
    if _SESSION_DEFAULT_ENGINE is not None:
        return _SESSION_DEFAULT_ENGINE

    env_value = os.getenv("DB2PQ_ENGINE")
    if env_value:
        return _normalize_engine(env_value)

    return "duckdb"
