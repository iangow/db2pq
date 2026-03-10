import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from db2pq.postgres.duckdb_pg import _ensure_postgres_extension, read_postgres_table


class _RecordingConnection:
    def __init__(self):
        self.calls = []

    def raw_sql(self, sql):
        self.calls.append(sql)


class EnsurePostgresExtensionTests(unittest.TestCase):
    def test_imports_then_loads_postgres_extension(self):
        con = _RecordingConnection()
        import_extension = Mock()

        with patch.dict(
            "sys.modules",
            {"duckdb_extensions": SimpleNamespace(import_extension=import_extension)},
        ):
            _ensure_postgres_extension(con)

        import_extension.assert_called_once_with("postgres_scanner")
        self.assertEqual(
            con.calls,
            ["LOAD postgres_scanner"],
        )

    def test_read_postgres_table_loads_extension_before_configuring_connection(self):
        events = []
        fake_con = Mock()
        fake_df = Mock()
        fake_con.read_postgres.return_value = fake_df

        def record_extension(_con):
            events.append("extension")

        def record_config(_con):
            events.append("config")

        with (
            patch("db2pq.postgres.duckdb_pg.ibis.duckdb.connect", return_value=fake_con),
            patch("db2pq.postgres.duckdb_pg._ensure_postgres_extension", side_effect=record_extension),
            patch("db2pq.postgres.duckdb_pg.configure_duckdb_connection", side_effect=record_config),
            patch("db2pq.postgres.duckdb_pg.apply_where_sql", return_value=fake_df),
            patch("db2pq.postgres.duckdb_pg.apply_keep_drop", return_value=fake_df),
        ):
            result = read_postgres_table(
                user="iangow",
                host="wrds-pgdata-ident-w.wharton.private",
                port=9737,
                database="wrds",
                schema="crsp",
                table_name="dsf",
            )

        self.assertIs(result, fake_df)
        self.assertEqual(events, ["extension", "config"])


if __name__ == "__main__":
    unittest.main()
