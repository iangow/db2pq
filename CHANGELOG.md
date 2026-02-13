# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `CONTRIBUTING.md` with development workflow and versioning guidance.
- `RELEASING.md` with a repeatable release checklist.
- `create_roles` support in `wrds_update_pg()` to create schema roles and grants.
- `wrds_schema` support in `wrds_update_pg()` to read from a source schema
  different from the destination schema.

### Changed

- Bumped version to `0.1.7` to begin the next patch cycle.
- Updated `README.md` to link project documentation files.
- `wrds_update_pg()` now returns `bool` (`True` on update, `False` when already
  up to date).
- `wrds_update_pg()` now uses safe boolean coercion for `col_types` boolean
  targets (for example, values like `0/1` in WRDS float columns).
- `keep`/`drop` filtering is now regex-based and consistent across
  `wrds_update_pq()` and `wrds_update_pg()` (drop filters apply first, then keep).

## [0.1.6] - 2026-02-12

### Changed

- Refined `README.md` with installation, environment, and quickstart guidance.
- Added WRDS SSH setup documentation for SAS-based metadata workflows.
- Improved API and output layout documentation.

[Unreleased]: https://github.com/iangow/db2pq/compare/0.1.6...HEAD
[0.1.6]: https://github.com/iangow/db2pq/releases/tag/0.1.6
