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
uv run pytest
```

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
