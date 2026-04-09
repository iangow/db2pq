from dataclasses import dataclass

from psycopg import sql
from .column_filter import filter_columns


@dataclass(frozen=True)
class QueryPlan:
    schema: str
    table: str
    columns: list[str]
    source_columns: list[str]
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


def count_wrds_rows(
    conn,
    *,
    schema: str,
    table: str,
    where: str | None = None,
    obs: int | None = None,
) -> int:
    qs = qident(conn, schema)
    qt = qident(conn, table)

    count_sql = f"SELECT COUNT(*) FROM {qs}.{qt}"
    if where:
        count_sql += f" WHERE {where}"

    with conn.cursor() as cur:
        cur.execute(count_sql)
        total_rows = int(cur.fetchone()[0])

    if obs is not None:
        total_rows = min(total_rows, int(obs))

    return total_rows

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


def _resolve_output_columns(
    source_columns: list[str],
    rename: dict[str, str] | None = None,
) -> list[str]:
    rename = rename or {}
    output_columns = [rename.get(column, column) for column in source_columns]

    seen: set[str] = set()
    duplicates: set[str] = set()
    for column in output_columns:
        if column in seen:
            duplicates.add(column)
        seen.add(column)

    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(f"rename would create duplicate output columns: {duplicate_list}")

    return output_columns


def _normalize_output_col_types(
    source_columns: list[str],
    rename: dict[str, str] | None = None,
    col_types: dict[str, str] | None = None,
) -> dict[str, str]:
    col_types = col_types or {}
    output_columns = set(_resolve_output_columns(source_columns, rename))

    unknown = sorted(set(col_types) - output_columns)
    if unknown:
        unknown_list = ", ".join(unknown)
        raise ValueError(
            "col_types keys must refer to selected output columns after rename: "
            f"{unknown_list}"
        )

    return dict(col_types)

def build_wrds_select_sql(
    *,
    conn,
    schema: str,
    table: str,
    source_columns: list[str],
    output_columns: list[str] | None = None,
    col_types: dict[str, str] | None = None,
    source_col_types: dict[str, str] | None = None,
    tz: str | None = None,
    obs: int | None = None,
    where: str | None = None,
    qualify: str | None = None,
) -> str:
    col_types = col_types or {}
    source_col_types = source_col_types or {}
    output_columns = output_columns or source_columns

    if len(source_columns) != len(output_columns):
        raise ValueError("source_columns and output_columns must have the same length")

    qs = qident(conn, schema)
    qt = qident(conn, table)
    qprefix = f"{qualify}." if qualify else ""
    qtz = qliteral(conn, tz) if tz else None

    select_items = []
    for source_name, output_name in zip(source_columns, output_columns):
        qsource = qident(conn, source_name)
        qoutput = qident(conn, output_name)
        source_expr = qsource
        src_type = source_col_types.get(source_name, "").strip().lower()
        if qtz and src_type == "timestamp without time zone":
            source_expr = f"({qsource} AT TIME ZONE {qtz})"
        if output_name in col_types:
            target_type = col_types[output_name]
            if _is_boolean_type(target_type):
                select_items.append(f"{_safe_boolean_cast_expr(source_expr)} AS {qoutput}")
            else:
                select_items.append(f"{source_expr}::{target_type} AS {qoutput}")
        else:
            select_items.append(f"{source_expr} AS {qoutput}")

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
    rename: dict[str, str] | None = None,
    keep=None,
    drop=None,
    tz: str | None = None,
    obs: int | None = None,
    where: str | None = None,
    qualified_alias: str | None = None,
) -> QueryPlan:
    source_columns = select_columns(all_cols, keep=keep, drop=drop)
    output_columns = _resolve_output_columns(source_columns, rename)
    col_types = _normalize_output_col_types(
        source_columns,
        rename=rename,
        col_types=col_types,
    )
    selected_types = [source_col_types.get(c, "").strip().lower() for c in source_columns]
    n_naive_ts = sum(t == "timestamp without time zone" for t in selected_types) if tz else 0
    n_tz_ts = sum(t == "timestamp with time zone" for t in selected_types) if tz else 0

    sql = build_wrds_select_sql(
        conn=conn,
        schema=schema,
        table=table,
        source_columns=source_columns,
        output_columns=output_columns,
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
            source_columns=source_columns,
            output_columns=output_columns,
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
        columns=output_columns,
        source_columns=source_columns,
        col_types=dict(col_types),
        source_col_types=dict(source_col_types),
        sql=sql,
        qualified_sql=qualified_sql,
        n_naive_ts=n_naive_ts,
        n_tz_ts=n_tz_ts,
    )

def select_columns(all_cols, *, keep=None, drop=None):
    return filter_columns(all_cols, keep=keep, drop=drop)
