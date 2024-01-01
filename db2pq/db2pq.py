import ibis
import os
import ibis.selectors as s
from ibis import _
from tempfile import TemporaryFile, NamedTemporaryFile
import pyarrow.parquet as pq 

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
    uri = "postgresql://%s@%s:%s/%s" % (user, host, port, database)
    df = con.read_postgres(uri = uri, table_name=table_name, schema=schema)
    
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
        For example, `col_types = {'permno':'integer', 'permco':'integer'}`.
    
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
             alt_table_name=alt_table_name,
             batched=batched,
             use_duckdb=use_duckdb)

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