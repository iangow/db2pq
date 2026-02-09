# db2pq/_utils.py
import os, getpass

def pg_defaults(user=None, host=None, database=None, port=None):
    user = user or os.getenv("PGUSER") or getpass.getuser()
    host = host or os.getenv("PGHOST", "localhost")
    database = database or os.getenv("PGDATABASE") or user
    port = int(port or os.getenv("PGPORT") or 5432)
    return user, host, database, port