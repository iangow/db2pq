#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

find docs/reference -maxdepth 1 -type f \
  \( -name '*.qmd' -o -name '_sidebar.yml' \) \
  -delete

uv run --extra docs python -c "import db2pq; print(f'Using db2pq from {db2pq.__file__}')"
uv run --extra docs quartodoc build --config docs/_quarto.yml
uv run --extra docs quarto render docs --use-freezer
