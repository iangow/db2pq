# Documentation

This repository includes a Quarto + `quartodoc` documentation site under `docs/`.

## Install the docs toolchain

These notes assume `uv` as the default Python package manager, not just for
this repository. The equivalent `pip` command is shown here because it maps
directly to the package extra:

```bash
python3 -m pip install -e ".[docs]"
```

`quartodoc` generates the API reference pages. Quarto renders the website.
The docs extra also installs Jupyter so Quarto can execute Python code chunks
embedded in `.qmd` pages.

If your main project environment is on Python 3.14, it is safer to build the
docs in a separate Python 3.11 or 3.12 virtualenv until `quartodoc` fully
settles its Python 3.14 support.

## Build the site

From the repository root:

```bash
quartodoc build --config docs/_quarto.yml
quarto render docs
```

Then publish using

```bash
quarto publish gh-pages docs
```

For local iteration:

```bash
quartodoc build --config docs/_quarto.yml --watch
quarto preview docs
```

## Content layout

- `docs/index.qmd`: site landing page
- `docs/authentication.qmd`: WRDS authentication and credential-handling guide
- `docs/data-management.qmd`: adapted research data-management guide
- `docs/using-parquet-with-polars.qmd`: downstream analysis guide for repository users
- `docs/wrds-to-pq.qmd`: WRDS to Parquet workflow guide
- `docs/wrds-to-pg.qmd`: WRDS to PostgreSQL workflow guide
- `docs/pg-to-pq.qmd`: PostgreSQL to Parquet workflow guide
- `docs/pq-to-pg.qmd`: Parquet to PostgreSQL workflow guide
- `docs/reference/`: generated API reference output
