# Library to convert PostgreSQL data to parquet files

This package was created to convert PostgreSQL data to parquet format.
This package has four major functions, one for each of three popular data formats, plus an "update" function that only updates if necessary.

 - `wrds_pg_to_pq()`: Exports a WRDS PostgreSQL table to a parquet file.
 - `db_to_pq()`: Exports a PostgreSQL table to a parquet file.
 - `db_schema_to_pq()`: Exports a PostgreSQL schema to parquet files.
 - `wrds_update_pq()`: A variant on `wrds_pg_to_pq()` that checks the "last modified" value for the relevant SAS file against that of the local parquet before getting new data from the WRDS PostgreSQL server.
 - `wrds_pg_to_pg()`: Exports a WRDS PostgreSQL table to another PostgreSQL database.
 
## Requirements

### 1. Python
The software uses Python 3 and depends on Ibis, `pyarrow` (Python API for Apache Arrow libraries), and Paramiko.
These dependencies are installed when you use Pip:

```bash
pip install db2pq --upgrade
```

### 2. A WRDS ID

To access WRDS non-interactively (e.g., from Python scripts), you must use
**SSH public-key authentication**.

WRDS provides a dedicated SSH endpoint for key-based authentication:

`wrds-cloud-sshkey.wharton.upenn.edu`

#### Step 1: Generate a modern SSH key (recommended)
WRDS supports modern SSH key types. We recommend **ed25519**:

`ssh-keygen -t ed25519 -C "your_wrds_id@wrds"`

Accept the default location (`~/.ssh/id_ed25519`).

You may use a passphrase if your SSH agent is running.
For unattended jobs (cron / CI), an empty passphrase may be required.

#### Step 2: Install the public key on WRDS
Copy your public key to the WRDS SSH-key host:

```
cat ~/.ssh/id_ed25519.pub | \
ssh your_wrds_id@wrds-cloud-sshkey.wharton.upenn.edu \
  "mkdir -p ~/.ssh && chmod 700 ~/.ssh && \
   cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

If `~/.ssh` does not exist on WRDS, the command above will create it.

#### Step 3: (Recommended) Configure SSH
Add an entry to `~/.ssh/config`:

```
Host wrds
    HostName wrds-cloud-sshkey.wharton.upenn.edu
    User your_wrds_id
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```
You can now connect with:

```
ssh wrds
```

This configuration is also used automatically by `paramiko`, enabling
password-less access from Python.

#### Troubleshooting
If SSH still prompts for a password, run:

```
ssh -vvv wrds
```

and confirm that `publickey` appears in the list of authentication methods.

`wrds2pg` uses `paramiko` to execute SAS code on WRDS via SSH.
Password-based authentication will not work in unattended scripts.

### 3. Environment variables

Environment variables that the code uses include:

- `WRDS_ID`: Your [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) ID.
- `DATA_DIR`: The local repository for parquet files.

Once can set these environment variables in (say) `~/.zprofile`:

```bash
export WRDS_ID="iangow"
export DATA_DIR="~/Dropbox/pq_data"
```

As an alternative to setting these environment variables, they can be passed as values of arguments `wrds_id` and `data_dir`, respectively, of the functions above.

### Report bugs
Author: Ian Gow, <iandgow@gmail.com>
