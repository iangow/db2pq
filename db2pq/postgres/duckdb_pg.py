from dataclasses import dataclass

from .comments import get_pg_conn
from .introspect import get_table_column_types, get_table_columns
from .select_sql import count_wrds_rows, plan_wrds_query


_DEFAULT_ARROW_BATCH_SIZE = 100_000
_MIN_ARROW_BATCH_SIZE = 5_000
_MAX_ARROW_BATCH_SIZE = 100_000
_TARGET_ARROW_BATCH_BYTES = 8 * 1024 * 1024


@dataclass
class DuckDBArrowQuery:
    connection: object
    relation: object
    total_rows: int | None = None
    progress_label: str | None = None
    arrow_batch_size: int = _DEFAULT_ARROW_BATCH_SIZE

    def fetch_arrow_reader(self):
        return self.relation.fetch_arrow_reader(batch_size=self.arrow_batch_size)

    def fetch_arrow_table(self):
        return self.relation.fetch_arrow_table()


def _duckdb_sql_string_literal(value: str) -> str:
    return value.replace("'", "''")


def _estimate_arrow_value_width(pg_type: str) -> int:
    t = pg_type.strip().lower()

    if t in {"bool", "boolean"}:
        return 1
    if t in {"smallint"}:
        return 2
    if t in {"integer", "int", "real", "date"}:
        return 4
    if t in {
        "bigint",
        "double precision",
        "timestamp without time zone",
        "timestamp with time zone",
        "time without time zone",
        "time with time zone",
    }:
        return 8
    if t in {"uuid"}:
        return 16
    if t in {"numeric", "decimal"}:
        return 32
    if t in {"json", "jsonb"}:
        return 64
    if "char" in t or t in {"text", "bytea", "xml"}:
        return 128
    if t.endswith("[]") or t in {"array"}:
        return 64

    return 16


def _estimate_arrow_batch_size(
    columns: list[str],
    source_col_types: dict[str, str],
) -> int:
    row_bytes = sum(_estimate_arrow_value_width(source_col_types.get(col, "")) for col in columns)
    row_bytes = max(row_bytes, 1)
    batch_size = _TARGET_ARROW_BATCH_BYTES // row_bytes
    batch_size = max(_MIN_ARROW_BATCH_SIZE, int(batch_size))
    batch_size = min(_MAX_ARROW_BATCH_SIZE, batch_size)
    return batch_size

def read_postgres_table(
    *,
    user,
    host,
    port,
    database,
    schema,
    table_name,
    col_types=None,
    obs=None,
    threads=None,
    keep=None,
    drop=None,
    where=None,
    tz="UTC",
):
    import duckdb

    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar;")
    con.execute("SET enable_progress_bar_print=false;")
    # Required for very large text columns/aggregates that exceed Arrow's
    # regular 2 GiB string buffer limit.
    con.execute("SET arrow_large_buffer_size=true;")
    if threads:
        con.execute(f"SET threads TO {int(threads)};")

    uri = f"postgres://{user}@{host}:{port}/{database}"
    with get_pg_conn(uri) as pg_conn:
        all_cols = get_table_columns(pg_conn, schema, table_name)
        source_col_types = get_table_column_types(pg_conn, schema, table_name)
        total_rows = count_wrds_rows(
            pg_conn,
            schema=schema,
            table=table_name,
            where=where,
            obs=obs,
        )
        plan = plan_wrds_query(
            conn=pg_conn,
            schema=schema,
            table=table_name,
            all_cols=all_cols,
            source_col_types=source_col_types,
            col_types=col_types,
            keep=keep,
            drop=drop,
            tz=tz,
            obs=obs,
            where=where,
            qualified_alias="wrds",
        )

    con.execute(
        f"ATTACH '{uri}' AS wrds (TYPE postgres, SCHEMA '{schema}')"
    )
    relation = con.sql(plan.qualified_sql)
    return DuckDBArrowQuery(
        connection=con,
        relation=relation,
        total_rows=total_rows,
        progress_label=f"{schema}.{table_name}",
        arrow_batch_size=_estimate_arrow_batch_size(plan.columns, plan.source_col_types),
    )


def read_postgres_query(
    *,
    uri: str,
    sql: str,
    threads=None,
):
    import duckdb

    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar;")
    con.execute("SET enable_progress_bar_print=false;")
    con.execute("SET arrow_large_buffer_size=true;")
    if threads:
        con.execute(f"SET threads TO {int(threads)};")

    attach_uri = _duckdb_sql_string_literal(uri)
    query_sql = _duckdb_sql_string_literal(sql)
    con.execute(f"ATTACH '{attach_uri}' AS pgdb (TYPE postgres)")
    relation = con.sql(
        f"SELECT * FROM postgres_query('pgdb', '{query_sql}')"
    )
    return DuckDBArrowQuery(connection=con, relation=relation)
