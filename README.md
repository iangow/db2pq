# Library to convert PostgreSQL data to parquet files

This package was created to convert PostgreSQL data to parquet format.
This package has four major functions, one for each of three popular data formats, plus an "update" function that only updates if necessary.

 - `wrds_pg_to_pq()`: Exports a WRDS PostgreSQL table to a parquet file.
 - `db_to_pq()`: Exports a PostgreSQL table to a parquet file.
 - `db_schema_to_pq()`: Exports a PostgreSQL schema to parquet files.
 - `wrds_update_pq()`: A variant on `wrds_pg_to_pq()` that checks the "last modified" value for the relevant SAS file against that of the local parquet before getting new data from the WRDS PostgreSQL server.

## Requirements

### 1. Python
The software uses Python 3 and depends on Ibis, `pyarrow` (Python API for Apache Arrow libraries), and Paramiko.
These dependencies are installed when you use Pip:

```bash
pip install db2pq --upgrade
```

### 2. A WRDS ID
To use public-key authentication to access WRDS, follow hints taken from [here](https://debian-administration.org/article/152/Password-less_logins_with_OpenSSH) to set up a public key.
Copy that key to the WRDS server from the terminal on your computer. 
(Note that this code assumes you have a directory `.ssh` in your home directory. If not, log into WRDS via SSH, then type `mkdir ~/.ssh` to create this.) 
Here's code to create the key and send it to WRDS:

```bash
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub | ssh $WRDS_ID@wrds-cloud-sshkey.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"
```

Use an empty passphrase in setting up the key so that the scripts can run without user intervention.

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
