from __future__ import annotations

import pyarrow.parquet as pq
import pyarrow.types as pat
import pytest

from db2pq import db_to_pq


@pytest.mark.parametrize(
    ("numeric_mode", "expected_kind"),
    [
        (None, "decimal"),
        ("decimal", "decimal"),
        ("float64", "floating"),
        ("text", "string"),
    ],
)
def test_db_to_pq_duckdb_numeric_mode_controls_numeric_output(
    pg_test_config,
    data_dir,
    require_source_table,
    numeric_mode,
    expected_kind,
):
    require_source_table("crsp", "msf_v2")

    pq_file = db_to_pq(
        table_name="msf_v2",
        schema="crsp",
        user=pg_test_config["user"],
        host=pg_test_config["host"],
        database=pg_test_config["src_db"],
        port=pg_test_config["port"],
        data_dir=data_dir,
        alt_table_name=f"msf_v2_duckdb_numeric_{numeric_mode or 'default'}",
        obs=25,
        engine="duckdb",
        numeric_mode=numeric_mode,
        keep=["permno", "mthprc", "mthcap", "mthret", "mthcumfacpr"],
    )

    table = pq.read_table(pq_file)

    for name in ["mthprc", "mthcap", "mthret", "mthcumfacpr"]:
        field_type = table.schema.field(name).type
        if expected_kind == "decimal":
            assert pat.is_decimal(field_type)
        elif expected_kind == "floating":
            assert pat.is_floating(field_type)
        else:
            assert pat.is_string(field_type) or pat.is_large_string(field_type)
