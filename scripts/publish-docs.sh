#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

./scripts/build-docs.sh
quarto publish gh-pages docs --no-render
