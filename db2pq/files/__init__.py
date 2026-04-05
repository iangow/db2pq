from importlib import import_module

_LAZY_SUBMODULES = {"parquet", "paths", "timestamps"}


def __getattr__(name):
    if name in _LAZY_SUBMODULES:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = sorted(_LAZY_SUBMODULES)
