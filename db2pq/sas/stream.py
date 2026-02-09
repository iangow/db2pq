# db2pq/sas/stream.py
import os
import warnings

def get_process(sas_code: str, *, wrds_id: str = None):
    """
    Execute SAS code on the WRDS server via SSH and return STDOUT as a stream.
    """
    try:
        import paramiko
    except ImportError as exc:
        raise ImportError(
            "Paramiko is required for SAS streaming. "
            "Install it via pip (`pip install paramiko`) or use "
            "the SAS optional dependency: pip install 'db2pq[sas]'"
        ) from exc

    wrds_id = wrds_id or os.getenv("WRDS_ID")
    if not wrds_id:
        raise ValueError(
            "wrds_id must be provided either as an argument or via the WRDS_ID environment variable"
        )

    client = paramiko.SSHClient()
    # optional: load host keys / suppress warnings
    with warnings.catch_warnings():
        warnings.filterwarnings(action="ignore", module=".*paramiko.*")
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect("wrds-cloud-sshkey.wharton.upenn.edu", username=wrds_id, compress=False)

    command = "qsas -stdio -noterminal"
    stdin, stdout, _ = client.exec_command(command)
    stdin.write(sas_code)
    stdin.close()

    # tell the server we’re done sending data
    stdout.channel.shutdown_write()
    return stdout

def proc_contents(table_name: str, sas_schema: str, *, wrds_id: str = None, encoding: str = "utf-8"):
    sas_code = (
        f"PROC CONTENTS data={sas_schema}.{table_name}(encoding='{encoding}');"
    )
    p = get_process(sas_code, wrds_id=wrds_id)
    return p.readlines()

def get_modified_str(table_name: str, sas_schema: str, *, wrds_id: str = None, encoding: str = None):
    lines = proc_contents(table_name, sas_schema, wrds_id=wrds_id, encoding=encoding or "utf-8")
    if not lines:
        print(f"Table {sas_schema}.{table_name} not found.")
        return None
    modified = ""
    next_row = False
    import re
    for line in lines:
        if next_row:
            line = re.sub(r"^\s+(.*)\s+$", r"\1", line)
            line = re.sub(r"\s+$", "", line)
            if not re.findall(r"Protection", line):
                modified += " " + line.rstrip()
            next_row = False
        if re.match(r"Last Modified", line):
            modified = re.sub(r"^Last Modified\s+(.*?)\s{2,}.*$", r"Last modified: \1", line).rstrip()
            next_row = True
    return modified