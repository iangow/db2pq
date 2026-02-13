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
- `update_schema(schema, ...)`
- `get_pq_files(schema, ...)`
- `get_modified_pq(file_name)`
- `pq_last_updated(data_dir=None)`
- `db_schema_tables(schema, ...)`
- `get_wrds_comment(table_name, schema, ...)`
- `get_pg_comment(table_name, schema, ...)`
- `wrds_update_pg(table_name, schema, ...)`

## Notes

- WRDS PostgreSQL access uses host `wrds-pgdata.wharton.upenn.edu` and port `9737`.
- `batched=True` (default) lowers memory usage for large tables.
- `col_types` can be used to cast selected columns before writing Parquet.

## Development

Run editable install in this repository:

```bash
pip install -e .
```

With optional SAS dependency:

```bash
pip install -e ".[sas]"
```

## License

MIT License. See `LICENSE`.
