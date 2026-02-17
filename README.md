# db2pq: export PostgreSQL and WRDS data to Parquet

`db2pq` is a Python library for moving data from PostgreSQL into Apache Parquet files.
It is designed for both general PostgreSQL sources and the WRDS PostgreSQL service.

## What it does

- Export a single PostgreSQL table to Parquet.
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

Install optional SAS support (used by `wrds_update_pq(..., use_sas=True)`):

```bash
pip install --upgrade "db2pq[sas]"
```

Install optional pandas support (needed for DataFrame outputs from
`pq_last_modified(...)`):

```bash
pip install --upgrade "db2pq[pandas]"
```

Install both optional SAS and pandas support:

```bash
pip install --upgrade "db2pq[sas,pandas]"
```

## Environment variables

`db2pq` supports explicit function arguments and environment-based defaults.

Connection defaults:

- `PGUSER`: PostgreSQL user (falls back to local OS user)
- `PGHOST`: PostgreSQL host (default: `localhost`)
- `PGDATABASE`: PostgreSQL database (default: `PGUSER`)
- `PGPORT`: PostgreSQL port (default: `5432`)

WRDS + output defaults:

- `WRDS_ID`: WRDS username (required for WRDS helpers unless passed directly)
- `DATA_DIR`: base directory where Parquet files are written

Example shell setup:

```bash
export WRDS_ID="your_wrds_id"
export DATA_DIR="$HOME/pq_data"
```

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

## Quickstart

### 1) Export a PostgreSQL table

```python
from db2pq import db_to_pq

pq_file = db_to_pq(
    table_name="my_table",
    schema="public",
    host="localhost",
    database="mydb",
)

print(pq_file)
```

### 2) Export a WRDS table to Parquet

```python
from db2pq import wrds_pg_to_pq

wrds_pg_to_pq(
    table_name="dsi",
    schema="crsp",
    wrds_id="your_wrds_id",  # or set WRDS_ID in the environment
)
```

### 3) Update only when WRDS data changed

```python
from db2pq import wrds_update_pq

wrds_update_pq(
    table_name="dsi",
    schema="crsp",
    wrds_id="your_wrds_id",
)
```

### 4) Export all tables in a PostgreSQL schema

```python
from db2pq import db_schema_to_pq

files = db_schema_to_pq(schema="public")
print(files)
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

## Public API

From `db2pq`:

- `db_to_pq(table_name, schema, ...)`
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
- `wrds_update_pg(table_name, schema, ...)`

`wrds_update_pq()` supports SQL-style filtering via `where`, for example:

`wrds_update_pq("funda", "comp", where="indfmt = 'INDL' AND datafmt = 'STD'")`

## Notes

- WRDS PostgreSQL access uses host `wrds-pgdata.wharton.upenn.edu` and port `9737`.
- `batched=True` (default) lowers memory usage for large tables.
- `col_types` can be used to cast selected columns before writing Parquet.
- `keep`/`drop` accept regex pattern(s) in both `wrds_update_pq()` and
  `wrds_update_pg()`. If both are supplied, `drop` is applied before `keep`.
- `tz` defaults to `"UTC"` in both update paths:
  `wrds_update_pq()` uses it to interpret source naive timestamps before writing
  timezone-aware UTC parquet timestamps; `wrds_update_pg()` converts source
  `timestamp without time zone` columns using `AT TIME ZONE '<tz>'`.

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
