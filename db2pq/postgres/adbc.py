from __future__ import annotations

from pathlib import Path
from threading import Lock

from .comments import get_pg_conn
from .introspect import (
    get_table_column_types,
    get_table_columns,
    get_table_numeric_bounds,
)
from .select_sql import count_wrds_rows, plan_wrds_query
from ..types import normalize_col_types


_ADBC_DATABASE_CACHE: dict[str, object] = {}
_ADBC_DATABASE_CACHE_LOCK = Lock()


def _require_adbc_driver():
    try:
        import adbc_driver_postgresql.dbapi as adbc_dbapi
    except ImportError as exc:
        raise ImportError(
            "ADBC export requires adbc-driver-postgresql. "
            'Install it with: pip install "db2pq[adbc]"'
        ) from exc
    return adbc_dbapi


def _get_cached_adbc_database(uri: str):
    import adbc_driver_postgresql
    import adbc_driver_manager.dbapi

    with _ADBC_DATABASE_CACHE_LOCK:
        db = _ADBC_DATABASE_CACHE.get(uri)
        if db is None:
            db = adbc_driver_manager.dbapi._SharedDatabase(
                adbc_driver_postgresql.connect(uri)
            )
            _ADBC_DATABASE_CACHE[uri] = db
        return db


def close_adbc_cached() -> None:
    """Close cached PostgreSQL ADBC database handles for this process."""
    with _ADBC_DATABASE_CACHE_LOCK:
        databases = list(_ADBC_DATABASE_CACHE.values())
        _ADBC_DATABASE_CACHE.clear()

    for db in databases:
        db.close()


def _merge_adbc_col_types(
    user_col_types: dict[str, str] | None,
    numeric_bounds: dict[str, tuple[int, int]],
    *,
    numeric_mode: str = "text",
) -> dict[str, str]:
    merged = dict(user_col_types or {})

    if numeric_mode not in {"text", "float64", "decimal"}:
        raise ValueError("numeric_mode must be one of 'text', 'float64', or 'decimal'")

    for column in numeric_bounds:
        if column in merged:
            continue
        if numeric_mode == "float64":
            merged[column] = "double precision"
        else:
            # Avoid driver-specific opaque Arrow extension types for NUMERIC.
            # decimal mode repairs eligible text-backed numerics after fetch.
            merged[column] = "text"

    return merged


def _decimal_columns_to_repair(
    numeric_bounds: dict[str, tuple[int, int]],
    user_col_types: dict[str, str] | None,
    *,
    numeric_mode: str,
) -> dict[str, tuple[int, int]]:
    if numeric_mode != "decimal":
        return {}

    user_col_types = user_col_types or {}
    return {
        column: (precision, scale)
        for column, (precision, scale) in numeric_bounds.items()
        if column not in user_col_types and 0 < precision <= 76 and scale >= 0
    }


def export_postgres_table_via_adbc(
    *,
    uri: str,
    schema: str,
    table_name: str,
    out_file,
    col_types: dict[str, str] | None = None,
    modified: str | None = None,
    obs: int | None = None,
    keep=None,
    drop=None,
    where: str | None = None,
    row_group_size: int = 1024 * 1024,
    tz: str = "UTC",
    numeric_mode: str = "text",
    adbc_batch_size_hint_bytes: int | None = None,
    adbc_use_copy: bool | None = None,
):
    from ..files.parquet import write_record_batch_reader_to_parquet

    adbc_dbapi = _require_adbc_driver()
    out_path = Path(out_file).expanduser()
    col_types = normalize_col_types(col_types, engine="postgres")

    with get_pg_conn(uri) as conn:
        all_cols = get_table_columns(conn, schema, table_name)
        source_col_types = get_table_column_types(conn, schema, table_name)
        numeric_bounds = get_table_numeric_bounds(conn, schema, table_name)
        total_rows = count_wrds_rows(
            conn,
            schema=schema,
            table=table_name,
            where=where,
            obs=obs,
        )
        decimal_columns = _decimal_columns_to_repair(
            numeric_bounds,
            col_types,
            numeric_mode=numeric_mode,
        )
        col_types = _merge_adbc_col_types(
            col_types,
            numeric_bounds,
            numeric_mode=numeric_mode,
        )
        plan = plan_wrds_query(
            conn=conn,
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
        )
        sql = plan.sql

    import adbc_driver_manager

    shared_adbc_db = _get_cached_adbc_database(uri)
    adbc_conn = adbc_driver_manager.dbapi.Connection(
        shared_adbc_db,
        adbc_driver_manager.AdbcConnection(shared_adbc_db._db),
        autocommit=False,
    )
    with adbc_conn:
        with adbc_conn.cursor() as cur:
            stmt_options = {}
            if adbc_batch_size_hint_bytes is not None:
                import adbc_driver_postgresql

                stmt_options[
                    adbc_driver_postgresql.StatementOptions.BATCH_SIZE_HINT_BYTES.value
                ] = int(adbc_batch_size_hint_bytes)
            if adbc_use_copy is not None:
                import adbc_driver_postgresql

                stmt_options[adbc_driver_postgresql.StatementOptions.USE_COPY.value] = (
                    "true" if adbc_use_copy else "false"
                )
            if stmt_options:
                cur._stmt.set_options(**stmt_options)
            cur.execute(sql)
            reader = cur.fetch_record_batch()
            wrote_rows = write_record_batch_reader_to_parquet(
                reader,
                out_path,
                modified=modified,
                row_group_size=row_group_size,
                tz=tz,
                decimal_columns=decimal_columns,
                total_rows=total_rows,
                progress_label=f"{schema}.{table_name}",
            )

    return str(out_path) if wrote_rows else None


def export_postgres_query_via_adbc(
    *,
    uri: str,
    sql: str,
    out_file,
    modified: str | None = None,
    row_group_size: int = 1024 * 1024,
    tz: str = "UTC",
    adbc_batch_size_hint_bytes: int | None = None,
    adbc_use_copy: bool | None = None,
    parquet_writer_kwargs: dict | None = None,
):
    from ..files.parquet import write_record_batch_reader_to_parquet

    _require_adbc_driver()
    out_path = Path(out_file).expanduser()

    import adbc_driver_manager

    shared_adbc_db = _get_cached_adbc_database(uri)
    adbc_conn = adbc_driver_manager.dbapi.Connection(
        shared_adbc_db,
        adbc_driver_manager.AdbcConnection(shared_adbc_db._db),
        autocommit=False,
    )
    with adbc_conn:
        with adbc_conn.cursor() as cur:
            stmt_options = {}
            if adbc_batch_size_hint_bytes is not None:
                import adbc_driver_postgresql

                stmt_options[
                    adbc_driver_postgresql.StatementOptions.BATCH_SIZE_HINT_BYTES.value
                ] = int(adbc_batch_size_hint_bytes)
            if adbc_use_copy is not None:
                import adbc_driver_postgresql

                stmt_options[adbc_driver_postgresql.StatementOptions.USE_COPY.value] = (
                    "true" if adbc_use_copy else "false"
                )
            if stmt_options:
                cur._stmt.set_options(**stmt_options)
            cur.execute(sql)
            reader = cur.fetch_record_batch()
            wrote_rows = write_record_batch_reader_to_parquet(
                reader,
                out_path,
                modified=modified,
                row_group_size=row_group_size,
                tz=tz,
                parquet_writer_kwargs=parquet_writer_kwargs,
            )

    return str(out_path) if wrote_rows else None
