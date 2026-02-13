from psycopg import sql

def qident(conn, name: str) -> str:
    return sql.Identifier(name).as_string(conn)

def _is_boolean_type(pg_type: str) -> bool:
    t = pg_type.strip().lower()
    return t in {"bool", "boolean"}

def _safe_boolean_cast_expr(qc: str) -> str:
    # Normalize common WRDS encodings (numeric/text/boolean) to PostgreSQL boolean.
    # Unrecognized non-null values become NULL rather than raising cast errors.
    return (
        "CASE "
        f"WHEN {qc} IS NULL THEN NULL "
        f"WHEN lower(trim(CAST({qc} AS VARCHAR))) IN ('t','true','y','yes','1','1.0') THEN TRUE "
        f"WHEN lower(trim(CAST({qc} AS VARCHAR))) IN ('f','false','n','no','0','0.0') THEN FALSE "
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
    obs: int | None = None,
    qualify: str | None = None,
) -> str:
    col_types = col_types or {}

    qs = qident(conn, schema)
    qt = qident(conn, table)
    qprefix = f"{qualify}." if qualify else ""

    select_items = []
    for c in columns:
        qc = qident(conn, c)
        if c in col_types:
            target_type = col_types[c]
            if _is_boolean_type(target_type):
                select_items.append(f"{_safe_boolean_cast_expr(qc)} AS {qc}")
            else:
                select_items.append(f"{qc}::{target_type} AS {qc}")
        else:
            select_items.append(qc)

    out = f"SELECT {', '.join(select_items)} FROM {qprefix}{qs}.{qt}"
    if obs is not None:
        out += f" LIMIT {int(obs)}"
    return out

def select_columns(all_cols, *, keep=None, drop=None):
    if keep and drop:
        raise ValueError("Use only one of keep or drop")

    if keep is not None:
        return list(keep)

    if drop is not None:
        drop_set = set(drop)
        return [c for c in all_cols if c not in drop_set]

    return list(all_cols)
