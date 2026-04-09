# Contributing

## Development setup

```bash
git clone git@github.com:iangow/db2pq.git
cd db2pq
uv sync --extra dev
```

Optional SAS support:

```bash
uv sync --extra dev --extra sas
```

Optional docs tooling:

```bash
uv sync --extra dev --extra docs
```

If you prefer `uv pip`, this is equivalent:

```bash
uv pip install -e ".[dev]"
```

## Local workflow

1. Create a branch for your change.
2. Make code and documentation updates together.
3. Commit with a clear message.
4. Open a pull request (or merge directly if you are maintaining solo).

Run tests with:

```bash
uv run python -m pytest
```

Database-backed integration tests are intended to run locally. They are marked
with `local_pg` automatically when they rely on the PostgreSQL test fixtures in
`tests/conftest.py`.

Run only the local PostgreSQL integration tests with:

```bash
uv run python -m pytest -m local_pg
```

Run everything except the local PostgreSQL integration tests with:

```bash
uv run python -m pytest -m "not local_pg"
```

These tests expect a locally accessible PostgreSQL instance and will skip
cleanly if the configured database is unavailable or a required source table is
missing. The default source database is `iangow`, and tests may depend on known
tables such as `crsp.msf_v2`.

## Notes for Agents

If you are an automated coding agent or AI assistant working in this
repository:

- Treat tests marked `local_pg` as local integration tests, not CI
  requirements.
- Do not assume a PostgreSQL test database exists unless the local environment
  indicates one is available.
- Prefer `uv run python -m pytest -m "not local_pg"` for quick validation when
  database access is unknown.
- Use `uv run python -m pytest -m local_pg` only when a local PostgreSQL
  database is expected to be available.
- If a `local_pg` test skips because the database or a required table is
  missing, that is expected behavior and should not be treated as a code
  failure.
- Some local integration tests depend on known source tables with expected
  schemas and types, including `crsp.msf_v2`.
- When adding new local integration tests, prefer the existing fixtures in
  `tests/conftest.py` and gate table-specific tests with
  `require_source_table(...)`.

You can override connection settings with:

```bash
export DB2PQ_TEST_SRC_PGUSER=...
export DB2PQ_TEST_SRC_PGHOST=...
export DB2PQ_TEST_SRC_PGPORT=...
export DB2PQ_TEST_SRC_DB=...
export DB2PQ_TEST_DST_PGUSER=...
export DB2PQ_TEST_DST_PGHOST=...
export DB2PQ_TEST_DST_PGPORT=...
export DB2PQ_TEST_DST_DB=...
```

Build the documentation site with:

```bash
./scripts/build-docs.sh
```

For local docs iteration, prefer:

```bash
uv run --extra docs quartodoc build --config docs/_quarto.yml --watch
uv run --extra docs quarto preview docs
```

These `uv` commands resolve `db2pq` from the local editable checkout, so docs
development uses in-repo changes rather than the latest PyPI release.

## Versioning

This project uses semantic versioning:

- Patch (`0.1.x`) for bug fixes and documentation-only changes.
- Minor (`0.x.0`) for backwards-compatible feature additions.
- Major (`x.0.0`) for breaking changes.

Update the version in `pyproject.toml` before tagging a new release.

## Documentation expectations

- Keep `README.md` accurate for installation and quickstart.
- Record user-visible changes in `CHANGELOG.md`.
- Follow `RELEASING.md` for tag and release creation.
