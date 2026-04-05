# Documentation

This repository includes a Quarto + `quartodoc` documentation site under `docs/`.

## Install the docs toolchain

```bash
python3 -m pip install -e ".[docs]"
```

`quartodoc` generates the API reference pages. Quarto renders the website.

If your main project environment is on Python 3.14, it is safer to build the
docs in a separate Python 3.11 or 3.12 virtualenv until `quartodoc` fully
settles its Python 3.14 support.

## Build the site

From the repository root:

```bash
quartodoc build --config docs/_quarto.yml
quarto render docs
```

For local iteration:

```bash
quartodoc build --config docs/_quarto.yml --watch
quarto preview docs
```

## Content layout

- `docs/index.qmd`: site landing page
- `docs/data-management.qmd`: adapted research data-management guide
- `docs/wrds-to-pq.qmd`: WRDS to Parquet workflow guide
- `docs/wrds-to-pg.qmd`: WRDS to PostgreSQL workflow guide
- `docs/pg-to-pq.qmd`: PostgreSQL to Parquet workflow guide
- `docs/pq-to-pg.qmd`: Parquet to PostgreSQL status page
- `docs/reference/`: generated API reference output
