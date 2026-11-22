# Notes for coding agents

`db2pq` moves PostgreSQL and WRDS tables into Parquet files under
`DATA_DIR/<schema>/<table>.parquet`. This file lists what an agent needs that
is not obvious from the README.

## Before calling anything

- Set `DB2PQ_NONINTERACTIVE=1`. Without it, a missing WRDS username or
  password makes the WRDS helpers wait on `input()` / `getpass()`.
  With it, the same situation raises an error that names the fix.
- Run `python -m db2pq doctor --json`. It reports the resolved `DATA_DIR`,
  whether that came from the environment or fell back to the current
  directory, whether `WRDS_ID` is set and whether `.pgpass` has a WRDS entry.
  It opens no connection and never prints a password.
- If `source` is `"cwd"`, stop and set `DATA_DIR`. Otherwise Parquet files are
  written into the working directory.

## Inspecting data without a database

- `python -m db2pq status --json` lists every Parquet file with row count,
  column count, size and the embedded `last_modified` string.
  `db2pq.pq_inventory()` returns the same rows as a list of dictionaries.
- `python -m db2pq check crsp.dsf comp.funda --json` exits 1 when a table is
  missing or unreadable. `db2pq.pq_check([...])` returns
  `{"ok", "present", "missing", "tables"}`. Neither creates directories.
- `pq_last_modified()` returns a pandas DataFrame and needs the `pandas`
  extra. Prefer `pq_inventory()`.

## Credentials

- Never read, print or log `~/.pgpass` or `WRDS_PASSWORD`.
- To store a WRDS password without a prompt, the user exports
  `WRDS_PASSWORD` for one command and runs
  `python -m db2pq save-wrds-password`.
- WRDS access needs a licence. If the user has none, do not try to work
  around it. Use `pq_check` to report what is missing and stop there.

## Return values

- `wrds_update_pq()` and `pg_update_pq()` return the Parquet path when a file
  was written and `None` otherwise. `None` covers three different outcomes,
  "already up to date", "no rows returned" and "source has no last-modified
  comment", which are told apart only by the printed message. Compare
  `pq_inventory()` before and after when the difference matters.
- A table that does not exist on WRDS prints a message and returns `None`.
  It does not raise.

## Development

- `uv sync --extra dev` then `uv run python -m pytest -m "not local_pg"`.
- Tests marked `local_pg` need a local PostgreSQL with the expected tables.
- After changing a public signature or docstring, run
  `uv run python scripts/generate_public_api.py`.
