name = "db2pq"
__version__ = "0.2.7"

from importlib import import_module

def _lazy_export(module_name, attr_name):
    def wrapper(*args, **kwargs):
        module = import_module(module_name, __name__)
        return getattr(module, attr_name)(*args, **kwargs)

    wrapper.__name__ = attr_name
    wrapper.__qualname__ = attr_name
    wrapper.__module__ = __name__
    wrapper.__doc__ = f"Lazy proxy for `{module_name}.{attr_name}`."
    return wrapper


db_to_pq = _lazy_export(".core", "db_to_pq")
db_to_pg = _lazy_export(".core", "db_to_pg")
ibis_to_pq = _lazy_export(".ibis", "ibis_to_pq")
wrds_pg_to_pq = _lazy_export(".core", "wrds_pg_to_pq")
wrds_pg_to_pg = _lazy_export(".core", "wrds_pg_to_pg")
db_schema_to_pq = _lazy_export(".core", "db_schema_to_pq")
wrds_update_pq = _lazy_export(".core", "wrds_update_pq")
pq_list_files = _lazy_export(".files.paths", "pq_list_files")
wrds_update_schema = _lazy_export(".core", "wrds_update_schema")

pq_last_modified = _lazy_export(".files.parquet", "pq_last_modified")
pq_archive = _lazy_export(".files.parquet", "pq_archive")
pq_restore = _lazy_export(".files.parquet", "pq_restore")
pq_remove = _lazy_export(".files.parquet", "pq_remove")
db_schema_tables = _lazy_export(".postgres.schema", "db_schema_tables")
wrds_update_pg = _lazy_export(".postgres.update", "wrds_update_pg")
postgres_write_pg = _lazy_export(".postgres.update", "postgres_write_pg")
close_adbc_cached = _lazy_export(".postgres.adbc", "close_adbc_cached")
set_default_engine = _lazy_export(".config", "set_default_engine")
get_default_engine = _lazy_export(".config", "get_default_engine")
get_wrds_username = _lazy_export(".credentials", "get_wrds_username")
get_wrds_conninfo = _lazy_export(".credentials", "get_wrds_conninfo")
ensure_wrds_id = _lazy_export(".credentials", "ensure_wrds_id")
ensure_wrds_access = _lazy_export(".credentials", "ensure_wrds_access")
find_pgpass_entry = _lazy_export(".credentials", "find_pgpass_entry")
has_pgpass_password = _lazy_export(".credentials", "has_pgpass_password")
save_password = _lazy_export(".credentials", "save_password")
ensure_wrds_credentials = _lazy_export(".credentials", "ensure_wrds_credentials")

__all__ = [
    "__version__",
    "db_to_pq", "db_to_pg", "ibis_to_pq", "wrds_pg_to_pq", "wrds_pg_to_pg",
    "db_schema_to_pq", "db_schema_tables",
    "wrds_update_pq", "pq_list_files", "wrds_update_schema",
    "pq_last_modified", "pq_archive", "pq_restore", "pq_remove",
    "wrds_update_pg", "postgres_write_pg", "close_adbc_cached",
    "set_default_engine", "get_default_engine",
    "get_wrds_username", "get_wrds_conninfo",
    "ensure_wrds_id", "ensure_wrds_access",
    "find_pgpass_entry", "has_pgpass_password",
    "save_password", "ensure_wrds_credentials",
]
