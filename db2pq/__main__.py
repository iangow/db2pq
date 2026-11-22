"""Command-line entry point: ``python -m db2pq``.

Every subcommand takes ``--json`` and never prompts, so the output can be read
by scripts and coding agents. No subcommand prints a password.
"""
from __future__ import annotations

import argparse
import json
import os
import sys


def _emit(payload, as_json: bool, text: str) -> None:
    print(json.dumps(payload, indent=2, default=str) if as_json else text)


def _cmd_doctor(args) -> int:
    from . import __version__
    from .config import get_default_engine
    from .credentials import find_pgpass_entry, get_wrds_conninfo, is_interactive
    from .inventory import data_dir_info
    from .postgres.wrds import resolve_wrds_id

    info = {"version": __version__, **data_dir_info(args.data_dir)}
    info["engine"] = get_default_engine()
    info["prompts_enabled"] = is_interactive()
    try:
        wrds_id = resolve_wrds_id(None)
    except ValueError:
        wrds_id = None
    info["wrds_id_set"] = wrds_id is not None
    info["wrds_password_env_set"] = bool(os.getenv("WRDS_PASSWORD"))
    info["wrds_pgpass_entry"] = (
        find_pgpass_entry(get_wrds_conninfo(wrds_id)).found if wrds_id else False
    )
    problems = []
    if info["source"] == "cwd":
        problems.append(
            "DATA_DIR is not set; Parquet files would be written under the "
            "current directory. Set DATA_DIR in the environment or in .env."
        )
    if not info["wrds_id_set"]:
        problems.append("WRDS_ID is not set; WRDS helpers need it (or wrds_id=...).")
    elif not info["wrds_pgpass_entry"]:
        problems.append(
            "No WRDS entry in .pgpass. Set WRDS_PASSWORD and run "
            "`python -m db2pq save-wrds-password`."
        )
    info["problems"] = problems
    lines = [f"{k}: {v}" for k, v in info.items() if k != "problems"]
    lines += [f"problem: {p}" for p in problems]
    _emit(info, args.json, "\n".join(lines))
    return 0


def _cmd_status(args) -> int:
    from .inventory import data_dir_info, pq_inventory

    rows = pq_inventory(args.schema, data_dir=args.data_dir)
    payload = {**data_dir_info(args.data_dir), "tables": rows}
    text = "\n".join(
        f"{r['schema']}.{r['table']}\t{r['n_rows']}\t{r['last_modified'] or ''}"
        for r in rows
    ) or f"No Parquet files under {payload['data_dir']}."
    _emit(payload, args.json, text)
    return 0


def _cmd_check(args) -> int:
    from .inventory import pq_check

    result = pq_check(args.tables, data_dir=args.data_dir)
    text = "\n".join(
        [f"present: {t}" for t in result["present"]]
        + [f"MISSING: {t}" for t in result["missing"]]
    )
    _emit(result, args.json, text)
    return 0 if result["ok"] else 1


def _cmd_save_wrds_password(args) -> int:
    from .credentials import (
        ensure_wrds_id,
        get_wrds_conninfo,
        get_wrds_password,
        save_password,
    )

    wrds_id = ensure_wrds_id(args.wrds_id, interactive=False)
    password = get_wrds_password()
    if not password:
        print(
            "WRDS_PASSWORD is not set. Export it for this one command; it is "
            "written to .pgpass and can then be removed from the environment.",
            file=sys.stderr,
        )
        return 2
    passfile = save_password(get_wrds_conninfo(wrds_id), password=password)
    _emit({"saved": True, "passfile": str(passfile)}, args.json, f"Saved to {passfile}.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m db2pq",
        description="Inspect a db2pq Parquet repository without a database connection.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("--data-dir", default=None, help="Defaults to DATA_DIR.")
        p.add_argument("--json", action="store_true", help="Print JSON.")

    p = sub.add_parser("doctor", help="Report the configuration db2pq will use.")
    common(p)
    p.set_defaults(func=_cmd_doctor)

    p = sub.add_parser("status", help="List Parquet files with rows and last-modified.")
    p.add_argument("--schema", default=None)
    common(p)
    p.set_defaults(func=_cmd_status)

    p = sub.add_parser(
        "check", help="Exit 1 if any of the given schema.table files is missing."
    )
    p.add_argument("tables", nargs="+", metavar="schema.table")
    common(p)
    p.set_defaults(func=_cmd_check)

    p = sub.add_parser(
        "save-wrds-password",
        help="Write WRDS_PASSWORD from the environment to .pgpass without a prompt.",
    )
    p.add_argument("--wrds-id", default=None)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=_cmd_save_wrds_password)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
