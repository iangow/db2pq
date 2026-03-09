#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

: "${WRDS_ID:?WRDS_ID must be set in the environment or .env}"

DB2PQ_GIT_REPO="${DB2PQ_GIT_REPO:-https://github.com/iangow/db2pq.git}"
DB2PQ_GIT_REF="${DB2PQ_GIT_REF:-codex/wrds-private-host}"

REMOTE_WRDS_ID="$(printf '%q' "$WRDS_ID")"
REMOTE_DB2PQ_GIT_REPO="$(printf '%q' "$DB2PQ_GIT_REPO")"
REMOTE_DB2PQ_GIT_REF="$(printf '%q' "$DB2PQ_GIT_REF")"

ssh "${WRDS_ID}@wrds-cloud-sshkey.wharton.upenn.edu" \
  "WRDS_ID=${REMOTE_WRDS_ID} DB2PQ_GIT_REPO=${REMOTE_DB2PQ_GIT_REPO} DB2PQ_GIT_REF=${REMOTE_DB2PQ_GIT_REF} bash -s" <<'REMOTE'
set -euo pipefail

SCRATCH_HOME="${HOME/#\/home/\/scratch}"
if [[ "$SCRATCH_HOME" == "$HOME" ]]; then
  echo "Expected remote HOME to start with /home, got: $HOME" >&2
  exit 1
fi

UV_ROOT="$SCRATCH_HOME/uv"
UV_CACHE_DIR="$SCRATCH_HOME/.cache/uv"
VENV_DIR="$SCRATCH_HOME/venvs/db2pq"
DATA_DIR="$SCRATCH_HOME/data/db2pq"
DUCKDB_HOME="$SCRATCH_HOME/.duckdb"
DUCKDB_TEMP_DIR="$DUCKDB_HOME/tmp"

mkdir -p "$UV_CACHE_DIR" "$VENV_DIR" "$DATA_DIR" "$DUCKDB_TEMP_DIR"

export UV_UNMANAGED_INSTALL="$UV_ROOT"
export UV_CACHE_DIR
export XDG_CACHE_HOME="$SCRATCH_HOME/.cache"
export PATH="$UV_ROOT:$PATH"
export DATA_DIR
export WRDS_ID
export DB2PQ_DUCKDB_HOME="$DUCKDB_HOME"
export DB2PQ_DUCKDB_TEMP_DIRECTORY="$DUCKDB_TEMP_DIR"
export PYTHONUNBUFFERED=1
export PYTHONFAULTHANDLER=1

if [[ ! -x "$UV_ROOT/uv" ]]; then
  curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$UV_ROOT" sh
fi

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  uv venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
uv pip install --upgrade "git+${DB2PQ_GIT_REPO}@${DB2PQ_GIT_REF}"

TMP_PY="$(mktemp "${SCRATCH_HOME}/db2pq-run-XXXXXX.py")"
trap 'rm -f "$TMP_PY"' EXIT

cat >"$TMP_PY" <<'PY'
import traceback

from db2pq import set_wrds_use_private, wrds_update_pq

set_wrds_use_private(True)

jobs = [
    ("ccmxpf_lnkhist", "crsp", {"col_types": {"lpermno": "int32", "lpermco": "int32"}}),
    ("stocknames", "crsp", {}),
    ("dsi", "crsp", {}),
    ("comphist", "crsp", {}),
    ("dsedelist", "crsp", {}),
    ("dseexchdates", "crsp", {}),
    ("dsedist", "crsp", {}),
    ("msi", "crsp", {}),
    ("mse", "crsp", {}),
    ("msf", "crsp", {}),
    ("erdport1", "crsp", {}),
    ("dsf", "crsp", {}),
    ("factors_daily", "ff", {}),
    ("company", "comp", {}),
    ("funda", "comp", {}),
    ("funda_fncd", "comp", {}),
    ("fundq", "comp", {}),
    ("r_auditors", "comp", {}),
    ("idx_daily", "comp", {}),
    ("aco_pnfnda", "comp", {}),
    ("seg_customer", "compseg", {}),
    ("names_seg", "compseg", {}),
]

for table_name, schema, kwargs in jobs:
    print(f"Starting {schema}.{table_name} with kwargs={kwargs}", flush=True)
    try:
        wrds_update_pq(table_name, schema, **kwargs)
        print(f"Finished {schema}.{table_name}", flush=True)
    except Exception:
        print(f"Failed {schema}.{table_name}", flush=True)
        traceback.print_exc()
        raise
PY

echo "Running db2pq workload on WRDS Cloud..."
python -c 'print("Python smoke test OK", flush=True)'
python -c 'import db2pq; print("db2pq import OK", flush=True)'
python "$TMP_PY"
REMOTE
