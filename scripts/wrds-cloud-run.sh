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
LOCAL_DATA_DIR="${DATA_DIR:-$PWD/db2pq-data}"
REMOTE_DATA_DIR="${REMOTE_DATA_DIR:-}"
SYNC_DATA="${SYNC_DATA:-1}"

REMOTE_WRDS_ID="$(printf '%q' "$WRDS_ID")"
REMOTE_DB2PQ_GIT_REPO="$(printf '%q' "$DB2PQ_GIT_REPO")"
REMOTE_DB2PQ_GIT_REF="$(printf '%q' "$DB2PQ_GIT_REF")"
REMOTE_REMOTE_DATA_DIR="$(printf '%q' "$REMOTE_DATA_DIR")"
SSH_LOG="$(mktemp)"

{
ssh "${WRDS_ID}@wrds-cloud-sshkey.wharton.upenn.edu" \
  "WRDS_ID=${REMOTE_WRDS_ID} DB2PQ_GIT_REPO=${REMOTE_DB2PQ_GIT_REPO} DB2PQ_GIT_REF=${REMOTE_DB2PQ_GIT_REF} REMOTE_DATA_DIR=${REMOTE_REMOTE_DATA_DIR} bash -s" <<'REMOTE'
set -euo pipefail

SCRATCH_HOME="${HOME/#\/home/\/scratch}"
if [[ "$SCRATCH_HOME" == "$HOME" ]]; then
  echo "Expected remote HOME to start with /home, got: $HOME" >&2
  exit 1
fi

if [[ -n "${REMOTE_DATA_DIR:-}" ]]; then
  DATA_DIR="$REMOTE_DATA_DIR"
else
  DATA_DIR="$SCRATCH_HOME/data/db2pq"
fi

UV_ROOT="$SCRATCH_HOME/uv"
UV_CACHE_DIR="$SCRATCH_HOME/.cache/uv"
VENV_DIR="$SCRATCH_HOME/venvs/db2pq"
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

echo "Resolved remote DATA_DIR: ${DATA_DIR}"
echo "DB2PQ_REMOTE_DATA_DIR=${DATA_DIR}"

JOB_ROOT="$SCRATCH_HOME/db2pq-batch"
JOB_LOG_DIR="$JOB_ROOT/logs"
mkdir -p "$JOB_LOG_DIR"

if [[ ! -x "$UV_ROOT/uv" ]]; then
  curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$UV_ROOT" sh
fi

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  uv venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
uv pip install --upgrade "git+${DB2PQ_GIT_REPO}@${DB2PQ_GIT_REF}"

python - <<'PY'
from duckdb_extensions import import_extension

import_extension("postgres_scanner")
print("DuckDB postgres_scanner extension import OK", flush=True)
PY

RUN_ID="$(date +%Y%m%d-%H%M%S)"
TMP_PY="$JOB_ROOT/db2pq-run-${RUN_ID}.py"
TMP_SH_NAME="db2pq-run-${RUN_ID}.sh"
TMP_SH="$HOME/${TMP_SH_NAME}"
STDOUT_LOG="$JOB_LOG_DIR/db2pq-run-${RUN_ID}.out"
STDERR_LOG="$JOB_LOG_DIR/db2pq-run-${RUN_ID}.err"

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

cat >"$TMP_SH" <<EOF
#!/bin/bash
#$ -cwd
#$ -pe onenode 4
#$ -l m_mem_free=6G
#$ -o ${STDOUT_LOG}
#$ -e ${STDERR_LOG}

set -euo pipefail

export UV_UNMANAGED_INSTALL="${UV_ROOT}"
export UV_CACHE_DIR="${UV_CACHE_DIR}"
export XDG_CACHE_HOME="${SCRATCH_HOME}/.cache"
export PATH="${UV_ROOT}:\$PATH"
export DATA_DIR="${DATA_DIR}"
export WRDS_ID="${WRDS_ID}"
export DB2PQ_DUCKDB_HOME="${DUCKDB_HOME}"
export DB2PQ_DUCKDB_TEMP_DIRECTORY="${DUCKDB_TEMP_DIR}"
export PYTHONUNBUFFERED=1
export PYTHONFAULTHANDLER=1

source "${VENV_DIR}/bin/activate"

echo "Starting WRDS batch job at \$(date)"
python -c 'print("Python smoke test OK", flush=True)'
python - <<'PY'
from duckdb_extensions import import_extension

import_extension("postgres_scanner")
print("DuckDB postgres_scanner extension import OK", flush=True)
PY
python -c 'import db2pq; print("db2pq import OK", flush=True)'
python "${TMP_PY}"
echo "Ending WRDS batch job at \$(date)"
EOF

chmod +x "$TMP_SH"

echo "Submitting WRDS batch job..."
echo "Batch wrapper: $TMP_SH"
cd "$HOME"
QSUB_OUTPUT="$(qsub "$TMP_SH_NAME")"
echo "$QSUB_OUTPUT"
JOB_ID="$(printf '%s\n' "$QSUB_OUTPUT" | sed -n 's/.*Your job \([0-9][0-9]*\).*/\1/p')"

if [[ -z "$JOB_ID" ]]; then
  echo "Failed to parse qsub job id." >&2
  exit 1
fi

echo "Tracking job ${JOB_ID}"
echo "stdout: ${STDOUT_LOG}"
echo "stderr: ${STDERR_LOG}"

while qstat | awk '{print $1}' | grep -qx "$JOB_ID"; do
  echo "Job ${JOB_ID} is still queued/running at $(date)"
  sleep 30
done

echo "Job ${JOB_ID} is no longer in qstat at $(date)"

if [[ -f "$STDOUT_LOG" ]]; then
  echo "Last 40 lines of stdout log:"
  tail -n 40 "$STDOUT_LOG"
fi

if [[ -f "$STDERR_LOG" ]]; then
  echo "Last 40 lines of stderr log:"
  tail -n 40 "$STDERR_LOG"
fi

rm -f "$TMP_SH"
REMOTE
} | tee "$SSH_LOG"

REMOTE_DATA_DIR_RESOLVED="$(grep '^DB2PQ_REMOTE_DATA_DIR=' "$SSH_LOG" | tail -n 1 | cut -d= -f2-)"
rm -f "$SSH_LOG"

if [[ -z "$REMOTE_DATA_DIR_RESOLVED" ]]; then
  echo "Failed to determine remote DATA_DIR from WRDS session output." >&2
  exit 1
fi

if [[ "$SYNC_DATA" != "0" ]]; then
  mkdir -p "$LOCAL_DATA_DIR"
  echo "Syncing ${REMOTE_DATA_DIR_RESOLVED} to ${LOCAL_DATA_DIR}"
  rsync -av --progress \
    "${WRDS_ID}@wrds-cloud-sshkey.wharton.upenn.edu:${REMOTE_DATA_DIR_RESOLVED}/" \
    "${LOCAL_DATA_DIR}/"
fi
