# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/iangow/db2pq/compare/0.2.2...HEAD
[0.2.2]: https://github.com/iangow/db2pq/releases/tag/0.2.2
[0.1.6]: https://github.com/iangow/db2pq/releases/tag/0.1.6
