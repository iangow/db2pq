import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from db2pq.postgres.duckdb_pg import _ensure_postgres_extension


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


if __name__ == "__main__":
    unittest.main()
