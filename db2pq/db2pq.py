import ibis
import os
import ibis.selectors as s
from ibis import _
from tempfile import TemporaryFile, NamedTemporaryFile
import pyarrow.parquet as pq
import re
import warnings
import paramiko
from pathlib import Path
from time import gmtime, strftime

client = paramiko.SSHClient()
wrds_id = os.getenv("WRDS_ID")
warnings.filterwarnings(action='ignore', module='.*paramiko.*')

def df_to_arrow(df, col_types=None, obs=None, batches=False):
    
    if col_types:
        types = set(col_types.values())
        for type in types:
            to_convert = [key for (key, value) in col_types.items() if value == type]
            df = df.mutate(s.across(to_convert, _.cast(type)))

    if obs:
        df = df.limit(obs)

    if batches:
        return df.to_pyarrow_batches()   
    else:
        return df.to_pyarrow()

def db_to_pq(table_name, schema, 
             user=os.getenv("PGUSER", default=os.getlogin()), 
             host=os.getenv("PGHOST", default="localhost"),
             database=os.getenv("PGDATABASE", default=os.getlogin()), 
             port=os.getenv("PGPORT", default=5432),
             data_dir=os.getenv("DATA_DIR", default=""),
             col_types=None,
             row_group_size=1048576,
             obs=None,
             modified=None,
             alt_table_name=None,
             batched=True):
    """Export a PostgreSQL table to a parquet file.

    Parameters
    ----------
    table_name: 
        Name of table in database.
    
    schema: 
        Name of database schema.

    host: string [Optional]
        Host name for the PostgreSQL server.
        The default is to use the environment value `PGHOST`.

    database: string [Optional]
        Name for the PostgreSQL database.
        The default is to use the environment value `PGDATABASE`
        or (if not set) user ID.
            
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    col_types: Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by PostgreSQL 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno':'integer', 'permco':'integer'}`.
    
    row_group_size: int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs: Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `db_to_pq()` with large tables.

    modified: string [Optional]
        Last modified string.
        
    alt_table_name: string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> db_to_pq("dsi", "crsp")
    >>> db_to_pq("feed21_bankruptcy_notification", "audit")
    """
    if not alt_table_name:
        alt_table_name = table_name
    
    con = ibis.duckdb.connect()
    uri = f"postgres://{user}@{host}:{port}/{database}"
    df = con.read_postgres(uri = uri, table_name=table_name, database=schema)
    data_dir = os.path.expanduser(data_dir)
    pq_dir = os.path.join(data_dir, schema)
    if not os.path.exists(pq_dir):
        os.makedirs(pq_dir)
    pq_file = os.path.join(data_dir, schema, alt_table_name + '.parquet')
    tmp_pq_file = os.path.join(data_dir, schema, '.temp_' + alt_table_name + '.parquet')
    
    if batched:
        # Get a few rows to infer schema for batched write
        tmpfile = TemporaryFile()
        df_arrow = df_to_arrow(df, col_types=col_types, obs=10)
        pq.write_table(df_arrow, tmpfile)
        schema = pq.read_schema(tmpfile)
        if modified:
            schema = schema.with_metadata({b'last_modified': modified.encode()})
        
        # Process data in batches
        with pq.ParquetWriter(tmp_pq_file, schema) as writer:
            batches = df_to_arrow(df, col_types=col_types, obs=obs, batches=True)
            for batch in batches:
                writer.write_batch(batch)
    else:
        df_arrow = df_to_arrow(df, col_types=col_types, obs=obs)
        pq.write_table(df_arrow, tmp_pq_file, row_group_size=row_group_size)
    
    os.rename(tmp_pq_file, pq_file)
    return pq_file

def wrds_pg_to_pq(table_name, 
                  schema, 
                  wrds_id=os.getenv("WRDS_ID", default=""),
                  data_dir=os.getenv("DATA_DIR", default=""),
                  col_types=None,
                  row_group_size=1048576,
                  obs=None,
                  modified=None,
                  alt_table_name=None,
                  batched=True):
    """Export a table from the WRDS PostgreSQL database to a parquet file.

    Parameters
    ----------
    table_name: 
        Name of table in database.
    
    schema: 
        Name of database schema.

    wrds_id: string
        WRDS ID to be used to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`.

    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    col_types: Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by PostgreSQL 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno': 'int32', 'permco': 'int32'}`.
    
    row_group_size: int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs: Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `db_to_pq()` with large tables.
    
    alt_table_name: string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> db_to_pq("dsi", "crsp")
    >>> db_to_pq("feed21_bankruptcy_notification", "audit")
    """
    db_to_pq(table_name, schema, user=wrds_id, 
             host="wrds-pgdata.wharton.upenn.edu",
             database="wrds",
             port=9737,
             data_dir=data_dir, 
             col_types=col_types,
             row_group_size=row_group_size,
             obs=obs,
             modified=modified,
             alt_table_name=alt_table_name,
             batched=batched)

def db_schema_tables(schema, 
                     user=os.getenv("PGUSER", default=os.getlogin()), 
                     host=os.getenv("PGHOST", default="localhost"),
                     database=os.getenv("PGDATABASE", default=os.getlogin()), 
                     port=os.getenv("PGPORT", default=5432)):
    """Get list of all tables in a PostgreSQL schema.

    Parameters
    ----------
    schema: 
        Name of database schema.

    user: string [Optional]
        User role for the PostgreSQL database.
        The default is to use the environment value `PGHOST`
        or (if not set) user ID.

    host: string [Optional]
        Host name for the PostgreSQL server.
        The default is to use the environment value `PGHOST`.

    database: string [Optional]
        Name for the PostgreSQL database.
        The default is to use the environment value `PGDATABASE`
        or (if not set) user ID.

    port: int [Optional]
        Port for the PostgreSQL server.
        The default is to use the environment value `PGPORT`
        or (if not set) 5432.
    
    Returns
    -------
    tables: list of strings
        Names of tables in schema.
    
    Examples
    ----------
    >>> db_schema_tables("crsp")
    >>> db_schema_tables("audit")
    """
    con = ibis.postgres.connect(user=user,    
                                host=host,
                                port=port,
                                database=database)
    tables = con.list_tables(schema=schema)
    return tables

def db_schema_to_pq(schema, 
                    user=os.getenv("PGUSER", default=os.getlogin()), 
                    host=os.getenv("PGHOST", default="localhost"),
                    database=os.getenv("PGDATABASE", default=os.getlogin()), 
                    port=os.getenv("PGPORT", default=5432),
                    data_dir=os.getenv("DATA_DIR", default=""),
                    row_group_size=1048576,
                    batched=True):
    """Export all tables in a PostgreSQL table to parquet files.

    Parameters
    ----------
    schema: 
        Name of database schema.

    user: string [Optional]
        User role for the PostgreSQL database.
        The default is to use the environment value `PGHOST`
        or (if not set) user ID.

    host: string [Optional]
        Host name for the PostgreSQL server.
        The default is to use the environment value `PGHOST`.

    database: string [Optional]
        Name for the PostgreSQL database.
        The default is to use the environment value `PGDATABASE`
        or (if not set) user ID.

    port: int [Optional]
        Port for the PostgreSQL server.
        The default is to use the environment value `PGPORT`
        or (if not set) 5432.
            
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    row_group_size: int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs: Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `db_to_pq()` with large tables.
    
    alt_table_name: string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
    
    Returns
    -------
    pq_files: list of strings
        Names of parquet files created.
    
    Examples
    ----------
    >>> db_schema_to_pq("crsp")
    >>> db_schema_to_pq("audit")
    """
    tables = db_schema_tables(schema, user, host, database, port)
    res = [db_to_pq(table_name=table_name, 
                    schema=schema, 
                    user=user, 
                    host=host,
                    database=database,
                    port=port,
                    data_dir=data_dir,
                    row_group_size=row_group_size,
                    batched=batched) for table_name in tables]
    return res

def get_process(sas_code, wrds_id=wrds_id, fpath=None):
    """Update a local CSV version of a WRDS table.

    Parameters
    ----------
    sas_code: 
        SAS code to be run to yield output. 
                      
    wrds_id: string
        Optional WRDS ID to be use to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`
    
    fpath: 
        Optional path to a local SAS file.
    
    Returns
    -------
    The STDOUT component of the process as a stream.
    """
    if client:
        client.close()

    if wrds_id:
        """Function runs SAS code on WRDS server and
        returns result as pipe on stdout."""
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect('wrds-cloud-sshkey.wharton.upenn.edu',
                       username=wrds_id, compress=False)
        command = "qsas -stdio -noterminal"
        stdin, stdout, stderr = client.exec_command(command)
        stdin.write(sas_code)
        stdin.close()

        channel = stdout.channel
        # indicate that we're not going to write to that channel anymore
        channel.shutdown_write()
        return stdout

def proc_contents(table_name, sas_schema=None, wrds_id=os.getenv("WRDS_ID"), 
                   encoding=None):
    if not encoding:
        encoding = "utf-8"
    
    sas_code = f"PROC CONTENTS data={sas_schema}.{table_name}(encoding='{encoding}');"

    p = get_process(sas_code, wrds_id)

    return p.readlines()

def get_modified_str(table_name, sas_schema, wrds_id=wrds_id,
                     encoding=None):
    
    contents = proc_contents(table_name=table_name, sas_schema=sas_schema, 
                             wrds_id=wrds_id, encoding=encoding)
    
    if len(contents) == 0:
        print(f"Table {sas_schema}.{table_name} not found.")
        return None

    modified = ""
    next_row = False
    for line in contents:
        if next_row:
            line = re.sub(r"^\s+(.*)\s+$", r"\1", line)
            line = re.sub(r"\s+$", "", line)
            if not re.findall(r"Protection", line):
                modified += " " + line.rstrip()
            next_row = False

        if re.match(r"Last Modified", line):
            modified = re.sub(r"^Last Modified\s+(.*?)\s{2,}.*$",
                              r"Last modified: \1", line)
            modified = modified.rstrip()
            next_row = True

    return modified

def get_modified_pq(file_name):
    
    if os.path.exists(file_name):
        md = pq.read_schema(file_name)
        schema_md = md.metadata
        if not schema_md:
            return ''
        if b'last_modified' in schema_md.keys():
            last_modified = schema_md[b'last_modified'].decode('utf-8')
        else:
            last_modified = ''
    else:
        last_modified = ''
    return last_modified

def wrds_update_pq(table_name, schema, 
                   wrds_id=os.getenv("WRDS_ID", default=""),
                   data_dir=os.getenv("DATA_DIR", default=""),
                   force=False,
                   col_types=None,
                   encoding="utf-8", 
                   sas_schema=None,
                   row_group_size=1048576,
                   obs=None,
                   alt_table_name=None,
                   batched=True):
    if not sas_schema:
        sas_schema = schema

    if not alt_table_name:
        alt_table_name = table_name
    
    pq_file = get_pq_file(table_name=table_name, schema=schema, 
                          data_dir=data_dir)
                
    modified = get_modified_str(table_name=table_name, 
                                sas_schema=sas_schema, wrds_id=wrds_id, 
                                encoding=encoding)
    if not modified:
        return False
    
    pq_modified = get_modified_pq(pq_file)
    if modified == pq_modified and not force:
        print(schema + "." + alt_table_name + " already up to date.")
        return False
    if force:
        print("Forcing update based on user request.")
    else:
        print("Updated %s.%s is available." % (schema, alt_table_name))
        print("Getting from WRDS.")
    
    print(f"Beginning file download at {get_now()} UTC.")
    wrds_pg_to_pq(table_name=table_name,
                  schema=schema,
                  data_dir=data_dir,
                  wrds_id=wrds_id,
                  col_types=col_types,
                  row_group_size=row_group_size,
                  obs=obs,
                  modified=modified,
                  alt_table_name=alt_table_name,
                  batched=batched)
    print(f"Completed file download at {get_now()} UTC.\n")

def get_pq_file(table_name, schema, data_dir=os.getenv("DATA_DIR")):
    
    data_dir = os.path.expanduser(data_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    schema_dir = Path(data_dir, schema)
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir)
        
    pq_file = Path(data_dir, schema, table_name).with_suffix('.parquet')
    return pq_file

def get_now():
    return strftime("%Y-%m-%d %H:%M:%S", gmtime())

def get_pq_files(schema, data_dir=os.getenv("DATA_DIR", default="")):
    """Get a list of parquet files in a schema.

    Parameters
    ----------
    schema: 
        Name of database schema.
            
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    Returns
    -------
    pq_files: [string]
        Names of parquet files found.
    """
    data_dir = os.path.expanduser(data_dir)
    pq_dir = os.path.join(data_dir, schema)
    files = os.listdir(pq_dir)
    return [re.sub(r"\.parquet$", "", pq_file) 
            for pq_file in files
            if re.search(r"\.parquet$", pq_file)]
            
def update_schema(schema, data_dir=os.getenv("DATA_DIR", default="")):
    """Update existing parquet files in a schema.

    Parameters
    ----------
    schema: 
        Name of database schema.
            
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    Returns
    -------
    pq_files: [string]
        Names of parquet files found.
    """
    pq_files = get_pq_files(schema = schema, data_dir = data_dir)
    for pq_file in pq_files:
        wrds_update_pq(table_name = pq_file, schema = schema, data_dir = data_dir)
