#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -x .venv/bin/quartodoc ]]; then
  QUARTODOC=.venv/bin/quartodoc
else
  QUARTODOC=quartodoc
fi

find docs/reference -maxdepth 1 -type f \
  \( -name '*.qmd' -o -name '_sidebar.yml' \) \
  -delete

$QUARTODOC build --config docs/_quarto.yml
quarto render docs --use-freezer
