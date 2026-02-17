from psycopg import sql
from .column_filter import filter_columns

def qident(conn, name: str) -> str:
    return sql.Identifier(name).as_string(conn)

def qliteral(conn, value: str) -> str:
    return sql.Literal(value).as_string(conn)

def _is_boolean_type(pg_type: str) -> bool:
    t = pg_type.strip().lower()
    return t in {"bool", "boolean"}

def _safe_boolean_cast_expr(expr: str) -> str:
    # Normalize common WRDS encodings (numeric/text/boolean) to PostgreSQL boolean.
    # Unrecognized non-null values become NULL rather than raising cast errors.
    return (
        "CASE "
        f"WHEN {expr} IS NULL THEN NULL "
        f"WHEN lower(trim(CAST({expr} AS VARCHAR))) IN ('t','true','y','yes','1','1.0') THEN TRUE "
        f"WHEN lower(trim(CAST({expr} AS VARCHAR))) IN ('f','false','n','no','0','0.0') THEN FALSE "
        "ELSE NULL "
        "END"
    )

def build_wrds_select_sql(
    *,
    conn,
    schema: str,
    table: str,
    columns: list[str],
    col_types: dict[str, str] | None = None,
    source_col_types: dict[str, str] | None = None,
    tz: str | None = None,
    obs: int | None = None,
    qualify: str | None = None,
) -> str:
    col_types = col_types or {}
    source_col_types = source_col_types or {}

    qs = qident(conn, schema)
    qt = qident(conn, table)
    qprefix = f"{qualify}." if qualify else ""
    qtz = qliteral(conn, tz) if tz else None

    select_items = []
    for c in columns:
        qc = qident(conn, c)
        source_expr = qc
        src_type = source_col_types.get(c, "").strip().lower()
        if qtz and src_type == "timestamp without time zone":
            source_expr = f"({qc} AT TIME ZONE {qtz})"
        if c in col_types:
            target_type = col_types[c]
            if _is_boolean_type(target_type):
                select_items.append(f"{_safe_boolean_cast_expr(source_expr)} AS {qc}")
            else:
                select_items.append(f"{source_expr}::{target_type} AS {qc}")
        else:
            select_items.append(f"{source_expr} AS {qc}")

    out = f"SELECT {', '.join(select_items)} FROM {qprefix}{qs}.{qt}"
    if obs is not None:
        out += f" LIMIT {int(obs)}"
    return out

def select_columns(all_cols, *, keep=None, drop=None):
    return filter_columns(all_cols, keep=keep, drop=drop)
