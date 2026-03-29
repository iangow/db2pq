# db2pq: export PostgreSQL and WRDS data to Parquet

`db2pq` is a Python library for moving data from PostgreSQL into Apache Parquet files.
It is designed for both general PostgreSQL sources and the WRDS PostgreSQL service.

## What it does

- Export a single PostgreSQL table to Parquet.
- Export an Ibis PostgreSQL table expression to Parquet.
- Export all tables in a PostgreSQL schema to Parquet.
- Export WRDS tables to Parquet.
- Update Parquet files only when the WRDS source table is newer.
- Mirror WRDS tables into a local PostgreSQL database.
- Read `last_modified` metadata embedded in Parquet files.

## Installation

Install from PyPI:

```bash
pip install --upgrade db2pq
```

This installs the bundled `psycopg` PostgreSQL client dependency, so most users
do not need a separate system `libpq` installation.

Install optional SAS support (used by `wrds_update_pq(..., use_sas=True)`):

```bash
pip install --upgrade "db2pq[sas]"
```

Install optional pandas support (needed for DataFrame outputs from
`pq_last_modified(...)`):

```bash
pip install --upgrade "db2pq[pandas]"
```

Install optional Ibis export support (needed for `ibis_to_pq(...)`):

```bash
pip install --upgrade "db2pq[ibis]"
```

Install optional ADBC export support (needed for `engine="adbc"` in
PostgreSQL-to-Parquet helpers):

```bash
pip install --upgrade "db2pq[adbc]"
```

Install both optional SAS and pandas support:

```bash
pip install --upgrade "db2pq[sas,pandas]"
```

## Environment variables

`db2pq` supports explicit function arguments and environment-based defaults.
It also loads a local `.env` file automatically (via `python-dotenv`) when resolving defaults.

Connection defaults:

- `PGUSER`: PostgreSQL user (falls back to local OS user)
- `PGHOST`: PostgreSQL host (default: `localhost`)
- `PGDATABASE`: PostgreSQL database (default: `PGUSER`)
- `PGPORT`: PostgreSQL port (default: `5432`)

WRDS + output defaults:

- `WRDS_ID`: WRDS username (required for WRDS helpers unless passed directly)
- `WRDS_USER`: accepted as a synonym for `WRDS_ID` for compatibility with Tidy Finance-style setups
- `WRDS_PASSWORD`: if present and no WRDS `.pgpass` entry exists, `db2pq` can offer to save it to `.pgpass`
- `DATA_DIR`: base directory where Parquet files are written

Example shell setup:

```bash
export WRDS_ID="your_wrds_id"
export DATA_DIR="$HOME/pq_data"
```

If `WRDS_ID` is not set, WRDS helpers such as `wrds_update_pq()` and
`wrds_pg_to_pq()` will prompt for it on first use and suggest adding it to a
local `.env` file in the calling project. If your WRDS PostgreSQL password is
not yet stored in `~/.pgpass` (or `PGPASSFILE`), `db2pq` will prompt for it
securely and save it for future connections. For compatibility with the Tidy
Finance Python setup, `db2pq` also recognizes `WRDS_USER` and can offer to
copy `WRDS_PASSWORD` into `.pgpass`.

## WRDS SSH setup (for SAS-based metadata)

`wrds_update_pq(..., use_sas=True)` uses SSH to execute SAS remotely. Configure
an SSH key for your WRDS account first:

```bash
ssh-keygen -t ed25519 -C "your_wrds_id@wrds"
cat ~/.ssh/id_ed25519.pub | \
ssh your_wrds_id@wrds-cloud-sshkey.wharton.upenn.edu \
"mkdir -p ~/.ssh && chmod 700 ~/.ssh && \
 cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

## Workflows

### 1) WRDS to Parquet

Use this path when WRDS is the source of truth and the Parquet repository is
the destination.

Most users should start with `wrds_update_pq()`, which updates only when the
WRDS source appears newer than the existing Parquet file:

```python
from db2pq import wrds_update_pq

wrds_update_pq("dsi", "crsp")
```

Use `wrds_pg_to_pq()` when you want a direct export without the update check:

```python
from db2pq import wrds_pg_to_pq

wrds_pg_to_pq(
    table_name="dsi",
    schema="crsp",
    wrds_id="your_wrds_id",  # or set WRDS_ID in the environment
    engine="adbc",
    numeric_mode="float64",
)
```

Common options on this path:

- `where` to filter rows before export
- `keep` / `drop` to select columns
- `alt_table_name` to rename the Parquet file
- `archive=True` to archive replaced files
- `use_sas=True` when you want SAS-based metadata for the update check

If `WRDS_ID` is missing, `db2pq` will ask for it and suggest adding
`WRDS_ID=...` to your project's `.env` file. If no matching WRDS password is
found in `.pgpass`, `db2pq` will prompt for your WRDS PostgreSQL password and
store it for next time.

### 2) WRDS to PostgreSQL

Use this path when you want to materialize WRDS tables into a local PostgreSQL
database first.

`wrds_update_pg()` is the main helper here:

```python
from db2pq import wrds_update_pg

wrds_update_pg(
    table_name="dsi",
    schema="crsp",
    dbname="research",
)
```

This path is useful when you want to:

- keep a local PostgreSQL mirror of selected WRDS tables
- build indexes or comments locally with `process_sql()` and `set_table_comment()`
- decouple downstream Parquet generation from live WRDS access

Related helpers:

- `wrds_get_tables(schema, wrds_id=None)` to list WRDS tables
- `process_sql(sql, ...)` to run SQL against the destination PostgreSQL database
- `set_table_comment(...)` to manage destination table comments

### 3) Local PostgreSQL to Parquet

Use this path when the source table is already in your local PostgreSQL
database, including tables created by `wrds_update_pg()`.

`pg_update_pq()` mirrors the behavior of `wrds_update_pq()` but reads from the
local PostgreSQL source:

```python
from db2pq import pg_update_pq

pg_update_pq(
    table_name="dsi",
    schema="crsp",
    database="research",
)
```

This path relies on the local PostgreSQL table comment for change detection.
If the table has no parseable last-modified comment, `pg_update_pq()` will not
export by default and will tell you to use `force=True`.

Use `db_to_pq()` when you want a direct local PostgreSQL to Parquet export
without the update check:

```python
from db2pq import db_to_pq

pq_file = db_to_pq(
    table_name="my_table",
    schema="public",
    host="localhost",
    database="mydb",
    engine="adbc",
    numeric_mode="float64",
)

print(pq_file)
```

`engine="duckdb"` is the recommended default. `engine="adbc"` is available,
but ADBC-based paths should currently be treated as experimental.

Use `db_schema_to_pq()` when you want to export an entire local PostgreSQL
schema:

```python
from db2pq import db_schema_to_pq

files = db_schema_to_pq(schema="public")
print(files)
```

### Other Export Path

`ibis_to_pq()` remains available when your starting point is an Ibis table
expression rather than a table name:

```python
from db2pq import ibis_to_pq

expr = con.table("my_table").filter(lambda t: t.id > 100)
ibis_to_pq(expr, "my_table.parquet", engine="duckdb", compression="zstd")
```

## Parquet layout

Files are organized as:

```text
<DATA_DIR>/<schema>/<table>.parquet
```

For example:

```text
/data/crsp/dsi.parquet
```

When `archive=True`, replaced files are moved under:

```text
<DATA_DIR>/<schema>/<archive_dir>/<table>_<timestamp>.parquet
```

## How it works

At a high level, the core PostgreSQL-to-Parquet flow has three stages:

1. Query planning
   `db2pq` inspects PostgreSQL metadata, applies `keep` / `drop`,
   normalizes user-supplied `col_types`, handles timestamp conversion rules,
   and builds a SQL `SELECT`.

2. Query execution
   The planned query is executed through either:
   - `engine="duckdb"`: DuckDB reads PostgreSQL and produces Arrow output
   - `engine="adbc"`: the PostgreSQL ADBC driver streams Arrow record batches directly

3. Parquet writing
   PyArrow writes the resulting Arrow batches/tables to Parquet, normalizing
   timestamps, repairing eligible decimal columns on the ADBC path, and
   buffering row groups with both row-count and byte-size limits.

This means the main export helpers now share the same SQL-planning logic even
when they use different execution engines.

### Engine defaults

`"duckdb"` remains the default engine. You can override it per call:

```python
db_to_pq("dsi", "crsp", engine="adbc")
```

or set a process-wide default:

```python
from db2pq import set_default_engine

set_default_engine("adbc")
```

You can inspect the current setting with `get_default_engine()`, and the
environment variable `DB2PQ_ENGINE` provides the same kind of session-level
default when you prefer configuration outside Python.

In general, DuckDB-backed paths are the stable default. ADBC-backed paths are
still experimental and may be faster or slower depending on the workload and
driver behavior. If you need the most reliable path, prefer `engine="duckdb"`.

## Public API

From `db2pq`:

- `db_to_pq(table_name, schema, ...)`
- `pg_update_pq(table_name, schema, ...)`
- `ibis_to_pq(table, out_file, engine=None, **writer_kwargs)`
- `wrds_pg_to_pq(table_name, schema, ...)`
- `db_schema_to_pq(schema, ...)`
- `wrds_update_pq(table_name, schema, ...)`
- `wrds_update_schema(schema, ...)`
- `pq_list_files(schema, data_dir=None, archive=False, archive_dir=None)`
- `pq_last_modified(table_name=None, schema=None, data_dir=None, file_name=None, archive=False, archive_dir="archive")`
- `pq_archive(table_name=None, schema=None, data_dir=None, file_name=None, archive_dir=None)`
- `pq_restore(file_basename, schema, data_dir=None, archive=True, archive_dir=None)`
- `pq_remove(table_name=None, schema=None, data_dir=None, file_name=None, archive=False, archive_dir="archive")`
- `db_schema_tables(schema, ...)`
- `wrds_get_tables(schema, wrds_id=None)`
- `wrds_update_pg(table_name, schema, ...)`
- `pq_to_pg(table_name, schema, ...)`
- `pq_update_pg(table_name, schema, ...)`
- `process_sql(sql, ...)`
- `set_table_comment(schema=..., table_name=..., comment=..., ...)`
- `get_wrds_username(wrds_id=None)`
- `get_wrds_conninfo(username=None)`
- `find_pgpass_entry(conninfo, **kwargs)`
- `has_pgpass_password(conninfo, **kwargs)`
- `save_password(conninfo, password=None, **kwargs)`
- `ensure_wrds_credentials(wrds_id=None, interactive=True)`
- `set_default_engine(engine)`
- `get_default_engine()`
- `close_adbc_cached()`

The main workflow entry points are:

- WRDS to Parquet: `wrds_update_pq()`, `wrds_pg_to_pq()`
- WRDS to PostgreSQL: `wrds_update_pg()`
- Local PostgreSQL to Parquet: `pg_update_pq()`, `db_to_pq()`, `db_schema_to_pq()`
- Parquet to PostgreSQL: `pq_update_pg()`, `pq_to_pg()`

`wrds_update_pq()` supports SQL-style filtering via `where`, for example:

`wrds_update_pq("funda", "comp", where="indfmt = 'INDL' AND datafmt = 'STD'")`

`pg_update_pq()` provides the same update-to-parquet pattern for a local
PostgreSQL source table, using the local table comment for change detection.

`pq_update_pg()` provides the reverse update path from a parquet file in the
local repository into PostgreSQL, using parquet `last_modified` metadata for
change detection.

## Notes

- WRDS PostgreSQL access uses host `wrds-pgdata.wharton.upenn.edu` and port `9737`.
- `batched=True` (default) lowers memory usage for large tables.
- `engine="adbc"` streams Arrow record batches directly from PostgreSQL into
  Parquet and may reduce RAM use versus the default DuckDB path.
- On the ADBC path, `numeric_mode="text"` casts PostgreSQL `NUMERIC` columns
  to `TEXT`, and `numeric_mode="float64"` casts them to `DOUBLE PRECISION`.
  `numeric_mode="decimal"` transports them as `TEXT` and converts eligible
  columns back to Arrow decimals using PostgreSQL precision/scale metadata.
  Columns without usable metadata remain strings. `col_types` still takes
  precedence over the mode.
- `col_types` can be used to cast selected columns before writing Parquet.
- `keep`/`drop` accept regex pattern(s) in both `wrds_update_pq()` and
  `wrds_update_pg()`. If both are supplied, `drop` is applied before `keep`.
- `tz` defaults to `"UTC"` in both update paths:
  `wrds_update_pq()` uses it to interpret source naive timestamps before writing
  timezone-aware UTC parquet timestamps; `wrds_update_pg()` converts source
  `timestamp without time zone` columns using `AT TIME ZONE '<tz>'`.
- `process_sql()` executes SQL against the PostgreSQL destination resolved from
  the same `PGUSER`/`PGHOST`/`PGDATABASE`/`PGPORT` defaults used by
  `wrds_update_pg()`.
- `set_table_comment()` can be used the same way for destination PostgreSQL
  table comments without passing an engine argument.

## Development

Run editable install in this repository:

```bash
pip install -e .
```

With optional SAS dependency:

```bash
pip install -e ".[sas]"
```

## Project docs

- Docs index: `docs/README.md`
- Contributor guide: `CONTRIBUTING.md`
- Release process: `RELEASING.md`
- Changelog: `CHANGELOG.md`

## License

MIT License. See `LICENSE`.
