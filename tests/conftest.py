from __future__ import annotations

import getpass
import os
from pathlib import Path

import psycopg
import pytest

from db2pq import close_adbc_cached


def _pg_setting(name: str, default: str) -> str:
    return os.getenv(name, default)


@pytest.fixture(scope="session")
def pg_test_config() -> dict[str, str | int]:
    user = _pg_setting("DB2PQ_TEST_PGUSER", os.getenv("PGUSER") or getpass.getuser())
    host = _pg_setting("DB2PQ_TEST_PGHOST", os.getenv("PGHOST") or "localhost")
    port = int(_pg_setting("DB2PQ_TEST_PGPORT", os.getenv("PGPORT") or "5432"))
    src_db = _pg_setting("DB2PQ_TEST_SRC_DB", "iangow")
    dst_db = _pg_setting("DB2PQ_TEST_DST_DB", "test")
    return {
        "user": user,
        "host": host,
        "port": port,
        "src_db": src_db,
        "dst_db": dst_db,
    }


def _connect_or_skip(uri: str):
    try:
        return psycopg.connect(uri)
    except Exception as exc:  # pragma: no cover - skip path depends on local env
        pytest.skip(f"PostgreSQL connection unavailable for integration test: {exc}")


@pytest.fixture(scope="session")
def src_pg_conn(pg_test_config):
    uri = (
        f"postgresql://{pg_test_config['user']}@{pg_test_config['host']}:"
        f"{pg_test_config['port']}/{pg_test_config['src_db']}"
    )
    with _connect_or_skip(uri) as conn:
        yield conn


@pytest.fixture(scope="session")
def dst_pg_conn(pg_test_config):
    uri = (
        f"postgresql://{pg_test_config['user']}@{pg_test_config['host']}:"
        f"{pg_test_config['port']}/{pg_test_config['dst_db']}"
    )
    with _connect_or_skip(uri) as conn:
        yield conn


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture(autouse=True)
def clear_adbc_cache():
    close_adbc_cached()
    yield
    close_adbc_cached()


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


@pytest.fixture
def require_source_table(src_pg_conn):
    def _require(schema: str, table: str) -> None:
        if not _table_exists(src_pg_conn, schema, table):
            pytest.skip(f"Source table {schema}.{table} is not available in the local test DB")

    return _require
