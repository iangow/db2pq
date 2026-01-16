import ibis
import os
import ibis.selectors as s
from ibis import _
import pyarrow.parquet as pq
import re
import warnings
import paramiko
from pathlib import Path
from time import gmtime, strftime
import pandas as pd
from datetime import datetime, time
from zoneinfo import ZoneInfo
import getpass

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

def db_to_pq(
    table_name,
    schema,
    *,
    user=None,
    host=None,
    database=None,
    port=None,
    data_dir=None,
    col_types=None,
    row_group_size=1048576,
    obs=None,
    modified=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    batched=True,
    threads=None,
    archive=False,
    archive_dir=None,
):
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
        or user.
            
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

    keep: string [Optional]
        Regular expression indicating columns to keep.
        
    drop: string [Optional]
        Regular expression indicating columns to drop.
    
    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
        
    threads: int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> db_to_pq("dsi", "crsp")
    >>> db_to_pq("feed21_bankruptcy_notification", "audit")
    """

    if user is None:
        user = os.getenv("PGUSER") or getpass.getuser()

    if host is None:
        host = os.getenv("PGHOST", "localhost")

    if database is None:
        database = os.getenv("PGDATABASE") or user

    if port is None:
        port = int(os.getenv("PGPORT") or 5432)

    if data_dir is None:
        data_dir = os.getenv("DATA_DIR", "")
    
    if not alt_table_name:
        alt_table_name = table_name
    
    con = ibis.duckdb.connect()
    if threads:
        con.raw_sql(f"SET threads TO {threads};")
        
    uri = f"postgres://{user}@{host}:{port}/{database}"
    df = con.read_postgres(uri, table_name=table_name, database=schema)
    data_dir = os.path.expanduser(data_dir)
    pq_dir = os.path.join(data_dir, schema)
    if not os.path.exists(pq_dir):
        os.makedirs(pq_dir)
    pq_file = os.path.join(data_dir, schema, alt_table_name + '.parquet')
    tmp_pq_file = os.path.join(data_dir, schema, '.temp_' + alt_table_name + '.parquet')
    
    if drop:
        df = df.drop(s.matches(drop))
        
    if keep:
        df = df.select(s.matches(keep))
    
    if batched:
        # Get a few rows to infer schema for batched write
        pq_schema = _infer_parquet_schema(df, col_types=col_types)
        if modified:
            pq_schema = pq_schema.with_metadata(
                {b'last_modified': modified.encode()})
        
        # Process data in batches
        with pq.ParquetWriter(tmp_pq_file, pq_schema) as writer:
            batches = df_to_arrow(df, col_types=col_types, obs=obs, batches=True)
            for batch in batches:
                writer.write_batch(batch)
    else:
        df_arrow = df_to_arrow(df, col_types=col_types, obs=obs)
        pq.write_table(df_arrow, tmp_pq_file, row_group_size=row_group_size)
    
    if archive and os.path.exists(pq_file):
        if not archive_dir:
            archive_dir = "archive"
        print(f"archive_dir: {archive_dir}")
        archive_path = os.path.join(data_dir, schema, archive_dir)
        if not os.path.exists(archive_path):
            os.makedirs(archive_path)
        modified_str =  parse_last_modified(get_modified_pq(pq_file))
        
        pq_file_archive =  os.path.join(data_dir, schema, archive_dir,
                                        alt_table_name + '_' +
                                        modified_str + '.parquet') 
        os.rename(pq_file, pq_file_archive)
    os.rename(tmp_pq_file, pq_file)
    return pq_file

def wrds_pg_to_pq(
    table_name,
    schema,
    *,
    wrds_id=None,
    data_dir=None,
    col_types=None,
    row_group_size=1048576,
    obs=None,
    modified=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    batched=True,
    threads=3,
    archive=False,
    archive_dir=None,
):
    """Export a table from the WRDS PostgreSQL database to a parquet file.

    Parameters
    ----------
    table_name: 
        Name of table in database.
    
    schema: 
        Name of database schema.

    wrds_id : string
        WRDS user ID used to access WRDS services.
        This parameter is required and must be provided either explicitly
        or via the `WRDS_ID` environment variable.

    data_dir : string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.
    
    col_types : Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by PostgreSQL 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno': 'int32', 'permco': 'int32'}`.
    
    row_group_size : int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    obs : Integer [Optional]
        Number of observations to import from database table.
        Implemented using SQL `LIMIT`.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `wrds_pg_to_pq()` with large tables.
    
    alt_table_name : string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    keep : string [Optional]
        Regular expression indicating columns to keep.
        
    drop : string [Optional]
        Regular expression indicating columns to drop.

    batched : bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
    
    threads : int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> wrds_pg_to_pq("dsi", "crsp")
    >>> wrds_pg_to_pq("feed21_bankruptcy_notification", "audit")
    """
    if wrds_id is None:
        wrds_id = os.getenv("WRDS_ID")
        if not wrds_id:
            raise ValueError(
                "wrds_id must be provided either as an argument or "
                "via the WRDS_ID environment variable"
            )

    if data_dir is None:
        data_dir = os.getenv("DATA_DIR", "")
    
    return db_to_pq(
        table_name,
        schema,
        user=wrds_id,
        host="wrds-pgdata.wharton.upenn.edu",
        database="wrds",
        port=9737,
        data_dir=data_dir,
        col_types=col_types,
        row_group_size=row_group_size,
        obs=obs,
        modified=modified,
        alt_table_name=alt_table_name,
        keep=keep,
        drop=drop,
        batched=batched,
        threads=threads,
        archive=archive,
        archive_dir=archive_dir,
    )

def db_schema_tables(
    schema,
    *,
    user=None,
    host=None,
    database=None,
    port=None,
):
    """Get list of all tables in a PostgreSQL schema.

    Parameters
    ----------
    schema :
        Name of the PostgreSQL database schema.

    user : string, optional
        PostgreSQL user role.
        If not provided, defaults to the value of the `PGUSER`
        environment variable, or (if unset) the current system user.

    host : string, optional
        Host name for the PostgreSQL server.
        If not provided, defaults to the value of the `PGHOST`
        environment variable, or `"localhost"` if unset.

    database : string, optional
        Name of the PostgreSQL database.
        If not provided, defaults to the value of the `PGDATABASE`
        environment variable, or (if unset) the resolved `user`.

    port : int, optional
        Port for the PostgreSQL server.
        If not provided, defaults to the value of the `PGPORT`
        environment variable, or `5432` if unset.
        
    Returns
    -------
    tables : list of strings
        Names of tables in schema.
    
    Examples
    ----------
    >>> db_schema_tables("crsp")
    >>> db_schema_tables("audit")
    """
    if user is None:
        user = os.getenv("PGUSER") or getpass.getuser()

    if host is None:
        host = os.getenv("PGHOST", "localhost")

    if database is None:
        database = os.getenv("PGDATABASE") or user

    if port is None:
        port = int(os.getenv("PGPORT") or 5432)

    con = ibis.postgres.connect(user=user, host=host, port=port, database=database)
    tables = con.list_tables(database=schema)
    return tables
    
def db_schema_to_pq(
    schema,
    *,
    user=None,
    host=None,
    database=None,
    port=None,
    data_dir=None,
    row_group_size=1048576,
    batched=True,
    threads=None,
    archive=False,
    archive_dir=None,
):
    """Export all tables in a PostgreSQL schema to Parquet files.

    Parameters
    ----------
    schema :
        Name of the PostgreSQL database schema.

    user : string, optional
        User role for the PostgreSQL database.
        If not provided, defaults to the value of the `PGUSER`
        environment variable, or (if unset) the current system user.

    host : string, optional
        Host name for the PostgreSQL server.
        If not provided, defaults to the value of the `PGHOST`
        environment variable, or `"localhost"` if unset.

    database : string, optional
        Name of the PostgreSQL database.
        If not provided, defaults to the value of the `PGDATABASE`
        environment variable, or (if unset) the resolved `user`.

    port : int, optional
        Port for the PostgreSQL server.
        If not provided, defaults to the value of the `PGPORT`
        environment variable, or `5432` if unset.

    data_dir : string, optional
        Root directory of the Parquet data repository.
        If not provided, defaults to the value of the `DATA_DIR`
        environment variable, or the current working directory.
    
    row_group_size: int [Optional]
        Maximum number of rows in each written row group. 
        Default is `1024 * 1024`.    
    
    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
            
    threads: int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    Returns
    -------
    pq_files: list of strings
        Names of parquet files created.
    
    Examples
    ----------
    >>> db_schema_to_pq("crsp")
    >>> db_schema_to_pq("audit")
    """
    if user is None:
        user = os.getenv("PGUSER") or getpass.getuser()

    if host is None:
        host = os.getenv("PGHOST", "localhost")

    if database is None:
        database = os.getenv("PGDATABASE") or user

    if port is None:
        port = int(os.getenv("PGPORT") or 5432)

    if data_dir is None:
        data_dir = os.getenv("DATA_DIR", "")

    tables = db_schema_tables(schema, user, host, database, port)
    res = [db_to_pq(table_name=table_name, 
                    schema=schema, 
                    user=user, 
                    host=host,
                    database=database,
                    port=port,
                    data_dir=data_dir,
                    row_group_size=row_group_size,
                    threads=threads,
                    batched=batched,
                    archive=archive,
                    archive_dir=archive_dir) for table_name in tables]
    return res

def get_process(sas_code, wrds_id=wrds_id, fpath=None):
    """Update a local CSV version of a WRDS table.

    Parameters
    ----------
    sas_code : 
        SAS code to be run to yield output. 
                      
    wrds_id : string
        WRDS user ID used to access WRDS services.
        This parameter is required and must be provided either explicitly
        or via the `WRDS_ID` environment variable.
    
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

def wrds_update_pq(
    table_name,
    schema,
    *,
    wrds_id=None,
    data_dir=None,
    force=False,
    col_types=None,
    encoding="utf-8",
    sas_schema=None,
    row_group_size=1048576,
    obs=None,
    alt_table_name=None,
    keep=None,
    drop=None,
    batched=True,
    threads=3,
    use_sas=False,
    archive=False,
    archive_dir=None,
):
    """Export a table from the WRDS PostgreSQL database to a Parquet file.

    Parameters
    ----------
    table_name :
        Name of the table in the WRDS PostgreSQL database.

    schema :
        Name of the database schema.

    wrds_id : string
        WRDS user ID used to access WRDS services.
        This parameter is required and must be provided either explicitly
        or via the `WRDS_ID` environment variable.

    data_dir : string, optional
        Root directory of the Parquet data repository.
        If not provided, defaults to the value of the `DATA_DIR`
        environment variable, or the current working directory.
        
    force: Boolean
        Whether update should proceed regardless of date comparison results.
    
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
        `wrds_update_pq()` with large tables.
    
    alt_table_name: string [Optional]
        Basename of parquet file. Used when file should have different name from `table_name`.

    keep: string [Optional]
        Regular expression indicating columns to keep.
        
    drop: string [Optional]
        Regular expression indicating columns to drop.

    batched: bool [Optional]
        Indicates whether data will be extracting in batches using
        `to_pyarrow_batches()` instead of a single call to `to_pyarrow()`.
        Using batches degrades performance slightly, but dramatically 
        reduces memory requirements for large tables.
                
    threads: int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    use_sas: bool [Optional]
        Should update get table comments from SAS data file.
        If False, then updated string comes from WRDS PostgreSQL table comment.
    
    Returns
    -------
    pq_file: string
        Name of parquet file created.
    
    Examples
    ----------
    >>> wrds_update_pq("dsi", "crsp")
    >>> wrds_update_pq("feed21_bankruptcy_notification", "audit")
    """                       
    if wrds_id is None:
        wrds_id = os.getenv("WRDS_ID")
        if not wrds_id:
            raise ValueError(
                "wrds_id must be provided either as an argument or "
                "via the WRDS_ID environment variable"
            )

    if data_dir is None:
        data_dir = os.getenv("DATA_DIR", "")
        
    if not sas_schema:
        sas_schema = schema

    if not alt_table_name:
        alt_table_name = table_name
    
    pq_file = get_pq_file(table_name=table_name, schema=schema, 
                          data_dir=data_dir)
                
    if use_sas:
        modified = get_modified_str(table_name=table_name, 
                                    sas_schema=sas_schema, wrds_id=wrds_id, 
                                    encoding=encoding)
    else:
        modified = get_wrds_comment(table_name=table_name, 
                                    schema=schema, wrds_id=wrds_id)

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
                  keep=keep,
                  drop=drop,
                  batched=batched,
                  threads=threads,
                  archive=archive,
                  archive_dir=archive_dir)
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
        
    threads: int [Optional]
        The number of threads DuckDB is allowed to use.
        Setting this may be necessary due to limits imposed on the user
        by the PostgreSQL database server.
    
    Returns
    -------
    pq_files: [string]
        Names of parquet files found.
    """
    pq_files = get_pq_files(schema=schema, data_dir=data_dir)
    for pq_file in pq_files:
        wrds_update_pq(table_name=pq_file, schema=schema, 
                       data_dir=data_dir, threads=3)
                       
def pq_last_updated(data_dir=None):
    """
    Get `last_updated` metadata for data files in a parquet data repository
    set up along the lines described at 
    https://iangow.github.io/far_book/parquet-wrds.html.

    Parameters
    ----------
    data_dir: string [Optional]
        Root directory of parquet data repository. 
        The default is to use the environment value `DATA_DIR` 
        or (if not set) the current directory.

    Returns
    -------
    df: [pd.DataFrame]
        Data frame with four columns: table, schema, last_mod_str, last_mod
    """
    
    if not data_dir:
        data_dir = os.path.expanduser(os.environ["DATA_DIR"])
    data_dir = Path(data_dir)
    
    df = pd.DataFrame([
        {"table": p.stem, 
         "schema": subdir.name, 
         "last_mod_str": get_modified_pq(p)}
        for subdir in data_dir.iterdir()
        if subdir.is_dir()
        for p in subdir.glob("*.parquet")
    ])

    df["last_mod"] = (
        df["last_mod_str"]
            .str.extract(r"^Last modified:\s*(.*)$", expand=False)
            .pipe(pd.to_datetime, errors="coerce")
            .dt.tz_localize("US/Eastern"))
    
    return df.sort_values("schema").reset_index(drop=True)                       

def get_pg_comment(
    table_name: str,
    schema: str,
    *,
    user: str | None = None,
    host: str | None = None,
    database: str | None = None,
    port: int | None = None,
) -> str | None:
    """Get the comment for a PostgreSQL object (table, view, etc.).

    Parameters
    ----------
    table_name :
        Name of the database object.

    schema :
        Name of the database schema.

    user : string, optional
        PostgreSQL user role.
        If not provided, defaults to the value of the `PGUSER`
        environment variable, or (if unset) the current system user.

    host : string, optional
        Host name for the PostgreSQL server.
        If not provided, defaults to the value of the `PGHOST`
        environment variable, or `"localhost"` if unset.

    database : string, optional
        Name of the PostgreSQL database.
        If not provided, defaults to the value of the `PGDATABASE`
        environment variable, or (if unset) the resolved `user`.

    port : int, optional
        Port for the PostgreSQL server.
        If not provided, defaults to the value of the `PGPORT`
        environment variable, or `5432` if unset.
    
    Returns
    -------
    comment: string
        Comment for PostgreSQL object.
    
    Examples
    ----------
    >>> get_pg_comment("dsf", "crsp")
    """
    if user is None:
        user = os.getenv("PGUSER") or getpass.getuser()

    if host is None:
        host = os.getenv("PGHOST", "localhost")

    if database is None:
        database = os.getenv("PGDATABASE") or user

    if port is None:
        port = int(os.getenv("PGPORT") or 5432)
    
    con = ibis.postgres.connect(user=user,    
                                host=host,
                                port=port,
                                database=database)
    
    sql = """
    SELECT obj_description(
             to_regclass(%(fqname)s),
             'pg_class'
           ) AS comment
    """
    fqname = f"{schema}.{table_name}"
    cur = con.raw_sql(sql, params={"fqname": fqname})
    try:
        row = cur.fetchone()
    finally:
        cur.close()
    return row[0] if row else None

def get_wrds_comment(table_name, schema, 
                     wrds_id=os.getenv("WRDS_ID", default="")):
    return get_pg_comment(table_name, schema, user=wrds_id, 
                          host="wrds-pgdata.wharton.upenn.edu",
                          database="wrds",
                          port=9737)

def parse_last_modified(s: str) -> str:
    """
    Return a filename-safe UTC timestamp stamp (YYYYMMDDTHHMMSSZ) from either:
      1) 'Last modified: 11/26/2025 01:40:41'  (America/New_York local time)
      2) '... (Updated 2026-01-07)'            (assume 02:00 America/New_York)

    Raises ValueError if no known pattern matches.
    """
    _NY = ZoneInfo("America/New_York")
    _UTC = ZoneInfo("UTC")

    _UPDATED_RE = re.compile(r"\(Updated\s+(\d{4}-\d{2}-\d{2})\)\s*$")
    
    s = s.strip()

    # Case 1: "Last modified: ..."
    if s.startswith("Last modified:"):
        ts = s.removeprefix("Last modified:").strip()
        dt_local = datetime.strptime(ts, "%m/%d/%Y %H:%M:%S").replace(tzinfo=_NY)

    # Case 2: "... (Updated yyyy-mm-dd)"
    else:
        m = _UPDATED_RE.search(s)
        if not m:
            raise ValueError(f"Unrecognized timestamp format: {s!r}")

        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        dt_local = datetime.combine(d, time(2, 0, 0), tzinfo=_NY)

    dt_utc = dt_local.astimezone(_UTC)
    return dt_utc.strftime("%Y%m%dT%H%M%SZ")

def _infer_parquet_schema(df, *, col_types):
    from tempfile import TemporaryFile
    with TemporaryFile() as tmp:
        arrow = df_to_arrow(df, col_types=col_types, obs=10)
        pq.write_table(arrow, tmp)
        tmp.seek(0)
        return pq.read_schema(tmp)
