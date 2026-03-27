from __future__ import annotations

import psycopg

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
