# Benchmarks

This directory contains ad hoc benchmark and probe scripts used during
development of `db2pq`.

These scripts are not part of the package API and are not run automatically as
part of the test suite. They are intended to answer focused questions about
performance, connection behavior, memory use, and output characteristics.

## Scripts

- `script_local_engine_benchmark.py`
  Compare local `db_to_pq()` performance across:
  - `engine="adbc"`
  - `engine="duckdb"`
  - PostgreSQL `pg_parquet`

  Current cases include:
  - `comp.company`
  - `comp.funda`
  - `crsp.dsf`

- `script_memory_benchmark.py`
  Measure peak RSS for local ADBC exports in a subprocess, to see whether
  memory usage scales with table size.

- `script_pg_to_pg_memory_probe.py`
  Run repeated large local PostgreSQL-to-PostgreSQL writes in a subprocess and
  report both process-tree peak RSS and per-iteration in-process RSS deltas.
  Defaults target:
  - source: `postgresql://localhost:5432/igow`
  - destination: `postgresql://localhost:5433/test`
  - tables: `comp.funda`, `crsp.dsf`
  Supported writers:
  - `db_to_pg`
  - `postgres_write_pg`
  - `wrds_update_pg` using a monkeypatched local source connection
  Useful env vars:
  - `DB2PQ_MEMPROBE_WRITERS`
  - `DB2PQ_MEMPROBE_ITERATIONS`
  - `DB2PQ_MEMPROBE_FUNDA_OBS`
  - `DB2PQ_MEMPROBE_DSF_OBS`

- `script_adbc_batch_profile.py`
  Inspect incoming ADBC batch sizes (rows and bytes per batch) for selected
  local tables.

- `script_adbc_cache_demo.py`
  Demonstrate the effect of the cached ADBC database handle across repeated
  WRDS update/export calls in a single Python session.

- `script_adbc_connect_probe.py`
  Stage-by-stage timing probe for the WRDS ADBC path, including metadata
  queries, `adbc_connect`, execution, and first-batch timing.

- `script_adbc_connect_split.py`
  Split the Python ADBC connection path into:
  - `adbc_driver_postgresql.connect(uri)`
  - `adbc_driver_manager.AdbcConnection(db)`

  This helps isolate where connection startup time is spent.

- `script_adbc_connect_variants.py`
  Compare ADBC and `psycopg` connection timing for WRDS using URI parameter
  variants such as `sslmode=require` and `gssencmode=disable`.

- `wrds_remote_small_benchmark.py`
  Small-row-count WRDS benchmark used to show the fixed startup cost of the
  Python ADBC connection path compared with DuckDB.

## Notes

- Most scripts assume a local PostgreSQL database named `iangow`.
- WRDS-focused scripts assume a valid `WRDS_ID` and working authentication.
- Some scripts write temporary Parquet files under `/tmp`.
- Output files such as `*.out` are not part of the benchmark definitions and
  can be kept elsewhere or ignored.
