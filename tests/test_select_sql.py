from __future__ import annotations

import pytest

import db2pq.postgres.select_sql as select_sql


@pytest.fixture(autouse=True)
def _simple_quoting(monkeypatch):
    monkeypatch.setattr(select_sql, "qident", lambda conn, name: f'"{name}"')
    monkeypatch.setattr(select_sql, "qliteral", lambda conn, value: f"'{value}'")


def test_plan_wrds_query_applies_rename_and_output_col_types():
    plan = select_sql.plan_wrds_query(
        conn=object(),
        schema="public",
        table="example",
        all_cols=["permno", "date", "ret"],
        source_col_types={
            "permno": "integer",
            "date": "date",
            "ret": "double precision",
        },
        keep=["permno", "ret"],
        rename={"ret": "return"},
        col_types={"return": "text"},
    )

    assert plan.source_columns == ["permno", "ret"]
    assert plan.columns == ["permno", "return"]
    assert plan.col_types == {"return": "text"}
    assert plan.sql == (
        'SELECT "permno" AS "permno", "ret"::text AS "return" '
        'FROM "public"."example"'
    )


def test_plan_wrds_query_rejects_duplicate_output_names():
    with pytest.raises(ValueError, match="duplicate output columns"):
        select_sql.plan_wrds_query(
            conn=object(),
            schema="public",
            table="example",
            all_cols=["permno", "lpermno"],
            source_col_types={"permno": "integer", "lpermno": "integer"},
            rename={"lpermno": "permno"},
        )


def test_plan_wrds_query_requires_col_types_to_use_output_names():
    with pytest.raises(ValueError, match="col_types keys must refer to selected output columns"):
        select_sql.plan_wrds_query(
            conn=object(),
            schema="public",
            table="example",
            all_cols=["permno", "ret"],
            source_col_types={"permno": "integer", "ret": "double precision"},
            rename={"ret": "return"},
            col_types={"ret": "text"},
        )
