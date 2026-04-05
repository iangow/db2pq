from __future__ import annotations


_TYPE_ALIASES = {
    "bool": "boolean",
    "boolean": "boolean",
    "int2": "int16",
    "smallint": "int16",
    "int16": "int16",
    "int4": "int32",
    "int": "int32",
    "integer": "int32",
    "int32": "int32",
    "int8": "int64",
    "bigint": "int64",
    "int64": "int64",
    "float4": "float32",
    "real": "float32",
    "float32": "float32",
    "float8": "float64",
    "double": "float64",
    "double precision": "float64",
    "float64": "float64",
    "text": "string",
    "string": "string",
    "large_string": "string",
    "varchar": "string",
    "character varying": "string",
    "char": "string",
    "character": "string",
    "utf8": "string",
    "date": "date",
    "date32": "date",
    "timestamp": "timestamp",
    "datetime": "timestamp",
    "timestamp without time zone": "timestamp",
    "bytea": "binary",
    "bytes": "binary",
    "binary": "binary",
}

_ENGINE_TYPE_MAPS = {
    "duckdb": {
        "boolean": "boolean",
        "int16": "int16",
        "int32": "int32",
        "int64": "int64",
        "float32": "float32",
        "float64": "float64",
        "string": "string",
        "date": "date",
        "timestamp": "timestamp",
        "binary": "binary",
    },
    "postgres": {
        "boolean": "boolean",
        "int16": "smallint",
        "int32": "integer",
        "int64": "bigint",
        "float32": "real",
        "float64": "double precision",
        "string": "text",
        "date": "date",
        "timestamp": "timestamp",
        "binary": "bytea",
    },
}


def normalize_col_types(
    col_types: dict[str, str] | None,
    *,
    engine: str,
) -> dict[str, str] | None:
    """Normalize user-facing type aliases for a specific engine."""
    if not col_types:
        return col_types

    try:
        engine_map = _ENGINE_TYPE_MAPS[engine]
    except KeyError as exc:
        raise ValueError(f"Unsupported engine for type normalization: {engine}") from exc

    normalized = {}
    for column, type_name in col_types.items():
        key = type_name.strip().lower()
        canonical = _TYPE_ALIASES.get(key)
        normalized[column] = engine_map.get(canonical, type_name)

    return normalized
