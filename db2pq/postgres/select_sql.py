from dataclasses import dataclass

from psycopg import sql
from .column_filter import filter_columns


@dataclass(frozen=True)
class QueryPlan:
    schema: str
    table: str
    columns: list[str]
    col_types: dict[str, str]
    source_col_types: dict[str, str]
    sql: str
    qualified_sql: str | None = None
    n_naive_ts: int = 0
    n_tz_ts: int = 0

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
    where: str | None = None,
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
    if where:
        out += f" WHERE {where}"
    if obs is not None:
        out += f" LIMIT {int(obs)}"
    return out


def plan_wrds_query(
    *,
    conn,
    schema: str,
    table: str,
    all_cols: list[str],
    source_col_types: dict[str, str],
    col_types: dict[str, str] | None = None,
    keep=None,
    drop=None,
    tz: str | None = None,
    obs: int | None = None,
    where: str | None = None,
    qualified_alias: str | None = None,
) -> QueryPlan:
    col_types = col_types or {}
    columns = select_columns(all_cols, keep=keep, drop=drop)
    selected_types = [source_col_types.get(c, "").strip().lower() for c in columns]
    n_naive_ts = sum(t == "timestamp without time zone" for t in selected_types) if tz else 0
    n_tz_ts = sum(t == "timestamp with time zone" for t in selected_types) if tz else 0

    sql = build_wrds_select_sql(
        conn=conn,
        schema=schema,
        table=table,
        columns=columns,
        col_types=col_types,
        source_col_types=source_col_types,
        tz=tz,
        obs=obs,
        where=where,
        qualify=None,
    )
    qualified_sql = None
    if qualified_alias:
        qualified_sql = build_wrds_select_sql(
            conn=conn,
            schema=schema,
            table=table,
            columns=columns,
            col_types=col_types,
            source_col_types=source_col_types,
            tz=tz,
            obs=obs,
            where=where,
            qualify=qualified_alias,
        )

    return QueryPlan(
        schema=schema,
        table=table,
        columns=columns,
        col_types=dict(col_types),
        source_col_types=dict(source_col_types),
        sql=sql,
        qualified_sql=qualified_sql,
        n_naive_ts=n_naive_ts,
        n_tz_ts=n_tz_ts,
    )

def select_columns(all_cols, *, keep=None, drop=None):
    return filter_columns(all_cols, keep=keep, drop=drop)
