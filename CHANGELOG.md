# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.7] - 2026-03-27

### Added

- Process-wide engine defaults via `set_default_engine()`,
  `get_default_engine()`, and `DB2PQ_ENGINE`.
- Cached PostgreSQL ADBC database handles via `close_adbc_cached()`.
- Shared `col_types` normalization so common Arrow-style and PostgreSQL-style
  type names work consistently across export and update paths.
- A local integration test suite for the core PostgreSQL paths.
- A `benchmarks/` directory with benchmark and probe scripts used during
  development.

### Changed

- Refactored PostgreSQL query planning into a shared layer used by the ADBC,
  DuckDB, and PostgreSQL update paths.
- Removed Ibis from the core DuckDB PostgreSQL-to-Parquet export flow.
- Improved ADBC Parquet writing by buffering with both row-count and byte-size
  limits, reducing wide-table memory blowups while preserving streamed writes.
- Refactored `ibis_to_pq()` to use the shared `engine="duckdb"` and
  `engine="adbc"` PostgreSQL export paths instead of its previous
  ADBC-only implementation.
- `wrds_update_pg()` now normalizes Arrow-style `col_types` such as `int32`
  into PostgreSQL casts such as `integer`.
- `wrds_update_pq()` update checks now respect `alt_table_name` correctly.
- Updated README documentation with a high-level architecture overview and
  engine-default behavior.
- Updated the local benchmark script to derive its default PostgreSQL database
  from the local connection defaults instead of a hard-coded username.
 
## [0.2.5] - 2026-03-23

### Added

- Added `ibis_to_pq()` as a public API for exporting PostgreSQL-backed
  Ibis table expressions directly to Parquet.
- Added the optional `db2pq[ibis]` extra for the ADBC PostgreSQL driver
  required by `ibis_to_pq()`.

### Changed

- Updated `README.md` to document Ibis-backed Parquet exports.

## [0.2.4] - 2026-03-18

### Changed

- Switched the core PostgreSQL dependency to `psycopg[binary]` so fresh
  installs work without a separate system `libpq` setup, especially on macOS.
- Declared Python 3.9 as the minimum supported version.
- Split `psycopg` dependency resolution by Python version:
  `psycopg[binary]>=3.3.3` on Python 3.10+ and `psycopg>=3.1,<3.3`
  on Python 3.9.

## [0.2.3] - 2026-03-15

### Changed

- Reduced import-time overhead across the package by switching top-level
  exports to lazy wrappers.
- Delayed loading of heavy dependencies such as `ibis`, `pyarrow`,
  `psycopg`, and `python-dotenv` until the specific functions that need
  them are called.
- Made `from db2pq import wrds_update_pq` substantially snappier by
  trimming work from both package import and first-call setup.

## [0.2.2] - 2026-03-05

### Added

- `python-dotenv` as a dependency for environment-based defaults.
- Automatic `.env` loading when resolving default `DATA_DIR` and `WRDS_ID`.

### Changed

- Centralized WRDS ID resolution through `resolve_wrds_id()` and reused it in
  core WRDS helpers and SAS streaming helpers.
- `pq_last_updated()` now uses the shared data directory resolver.
- Updated `README.md` to document automatic `.env` loading behavior.

## [0.1.6] - 2026-02-12

### Changed

- Refined `README.md` with installation, environment, and quickstart guidance.
- Added WRDS SSH setup documentation for SAS-based metadata workflows.
- Improved API and output layout documentation.

[Unreleased]: https://github.com/iangow/db2pq/compare/0.2.7...HEAD
[0.2.7]: https://github.com/iangow/db2pq/releases/tag/0.2.7
[0.2.5]: https://github.com/iangow/db2pq/releases/tag/0.2.5
[0.2.4]: https://github.com/iangow/db2pq/releases/tag/0.2.4
[0.2.3]: https://github.com/iangow/db2pq/releases/tag/0.2.3
[0.2.2]: https://github.com/iangow/db2pq/releases/tag/0.2.2
[0.1.6]: https://github.com/iangow/db2pq/releases/tag/0.1.6
