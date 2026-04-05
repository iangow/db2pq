from __future__ import annotations

from contextlib import nullcontext
import uuid

from db2pq import wrds_get_tables


def test_wrds_get_tables_lists_schema_tables(monkeypatch, pg_test_config, require_source_table):
    require_source_table("public", "example")

    src_uri = (
        f"postgresql://{pg_test_config['src_user']}@{pg_test_config['src_host']}:"
        f"{pg_test_config['src_port']}/{pg_test_config['src_db']}"
    )

    import db2pq.postgres.schema as schema_mod

    monkeypatch.setattr(schema_mod, "get_wrds_conn", lambda wrds_id=None: psycopg.connect(src_uri))

    tables = wrds_get_tables("public", wrds_id="ignored")

    assert "example" in tables


def test_wrds_get_tables_includes_views_when_requested(monkeypatch, src_pg_conn):
    view_name = f"example_view_{uuid.uuid4().hex[:8]}"

    import db2pq.postgres.schema as schema_mod

    monkeypatch.setattr(schema_mod, "get_wrds_conn", lambda wrds_id=None: nullcontext(src_pg_conn))

    with src_pg_conn.cursor() as cur:
        cur.execute(f'CREATE VIEW "public"."{view_name}" AS SELECT 1 AS id')
    src_pg_conn.commit()

    try:
        tables = wrds_get_tables("public", wrds_id="ignored")
        tables_and_views = wrds_get_tables("public", wrds_id="ignored", views=True)

        assert view_name not in tables
        assert view_name in tables_and_views
    finally:
        with src_pg_conn.cursor() as cur:
            cur.execute(f'DROP VIEW IF EXISTS "public"."{view_name}"')
        src_pg_conn.commit()
