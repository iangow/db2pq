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

if [[ ! -x "$UV_ROOT/uv" ]]; then
  curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$UV_ROOT" sh
fi

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  uv venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
uv pip install --upgrade "git+${DB2PQ_GIT_REPO}@${DB2PQ_GIT_REF}"

uv run --active python - <<'PY'
from db2pq import set_wrds_use_private, wrds_update_pq

set_wrds_use_private(True)

# CRSP
wrds_update_pq("ccmxpf_lnkhist", "crsp",
               col_types={"lpermno": "int32",
                          "lpermco": "int32"})
wrds_update_pq("stocknames", "crsp")
wrds_update_pq("dsi", "crsp")
wrds_update_pq("comphist", "crsp")
wrds_update_pq("dsedelist", "crsp")
wrds_update_pq("dseexchdates", "crsp")
wrds_update_pq("dsedist", "crsp")
wrds_update_pq("msi", "crsp")
wrds_update_pq("mse", "crsp")
wrds_update_pq("msf", "crsp")
wrds_update_pq("erdport1", "crsp")
wrds_update_pq("dsf", "crsp")

# Fama-French library
wrds_update_pq("factors_daily", "ff")

# Compustat
wrds_update_pq("company", "comp")
wrds_update_pq("funda", "comp")
wrds_update_pq("funda_fncd", "comp")
wrds_update_pq("fundq", "comp")
wrds_update_pq("r_auditors", "comp")
wrds_update_pq("idx_daily", "comp")
wrds_update_pq("aco_pnfnda", "comp")

# compseg
wrds_update_pq("seg_customer", "compseg")
wrds_update_pq("names_seg", "compseg")
PY
REMOTE
