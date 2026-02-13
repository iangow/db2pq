# Releasing db2pq

This checklist keeps package version, git tags, and GitHub releases aligned.

## 1) Prepare release content

1. Ensure `README.md` and docs are up to date.
2. Update `CHANGELOG.md`:
   - Move user-visible items from `Unreleased` to a new version section.
   - Use ISO date format (`YYYY-MM-DD`).
   - Keep `Unreleased` in place for ongoing work.

## 2) Bump version

Edit `pyproject.toml`:

```toml
[project]
version = "X.Y.Z"
```

Commit the version bump and changelog updates.

## 3) Tag

Create an annotated tag on `main`:

```bash
git checkout main
git pull
git tag -a X.Y.Z -m "Release X.Y.Z"
git push origin main
git push origin X.Y.Z
```

## 4) Create GitHub release

If GitHub CLI (`gh`) is installed and authenticated:

```bash
gh release create X.Y.Z \
  --draft \
  --title "db2pq vX.Y.Z"
```

Or create the release manually at:
`https://github.com/iangow/db2pq/releases/new`

Use the matching `CHANGELOG.md` version section as the release notes body.

## 5) Publish to PyPI (if needed)

Build:

```bash
python -m build
```

Upload:

```bash
python -m twine upload dist/*
```

## 6) Post-release

- Verify install of the released version from PyPI.
- Add a new `Unreleased` section if needed.
- Bump to the next development version when you are ready.
