from psycopg import sql

def qident(conn, name: str) -> str:
    return sql.Identifier(name).as_string(conn)

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
            select_items.append(f"{qc}::{col_types[c]} AS {qc}")
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
