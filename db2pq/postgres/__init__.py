from importlib import import_module

_LAZY_SUBMODULES = {
    "_defaults",
    "adbc",
    "column_filter",
    "comments",
    "copy",
    "duckdb_ddl",
    "duckdb_pg",
    "introspect",
    "schema",
    "select_sql",
    "update",
    "wrds",
}


def __getattr__(name):
    if name in _LAZY_SUBMODULES:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = sorted(_LAZY_SUBMODULES)
