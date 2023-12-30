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

def pg_to_pq(table_name, schema, 
             user=os.getenv("PGUSER", default=os.getlogin()), 
             host=os.getenv("PGHOST", default="localhost"),
             database=os.getenv("PGDATABASE", default=os.getlogin()), 
             port=5432,
             data_dir="",
             col_types=None,
             row_group_size=1048576,
             obs=None,
             alt_table_name=None,
             batched=True,
             use_duckdb=True):

    if not alt_table_name:
        alt_table_name = table_name
    
    if use_duckdb:
        con = ibis.duckdb.connect()
        uri = "postgresql://%s@%s:%s/%s" % (user, host, port, database)
        df = con.read_postgres(uri = uri, table_name=table_name, schema=schema)
    else:
        if batched:
            print("If use_duckdb is False, then batched must be False too.")
            batched = False
            
        con = ibis.postgres.connect(user=user,    
                                    host=host,
                                    port=port,
                                    database=database)
        df = con.table(name=table_name, schema=schema)

    data_dir = os.path.expanduser(data_dir)
    pq_file = os.path.join(data_dir, schema, alt_table_name + '.parquet')
    tmp_pq_file = NamedTemporaryFile(delete=False, suffix=".parquet")
    tmp_pq_filename = tmp_pq_file.name
    
    if batched:
        # Get a few rows to infer schema for batched write
        tmpfile = TemporaryFile()
        df_arrow = df_to_arrow(df, col_types=col_types, obs=10)
        pq.write_table(df_arrow, tmpfile)
        schema = pq.read_schema(tmpfile)
        
        # Process data in batches
        with pq.ParquetWriter(tmp_pq_filename, schema) as writer:
            batches = df_to_arrow(df, col_types=col_types, obs=obs, batches=True)
            for batch in batches:
                writer.write_batch(batch)
    else:
        df_arrow = df_to_arrow(df, col_types=col_types, obs=obs)
        pq.write_table(df_arrow, tmp_pq_filename, row_group_size=row_group_size)
    
    os.rename(tmp_pq_filename, pq_file)

def wrds_pg_to_pq(table_name, schema, 
               wrds_id,
               data_dir="", 
               col_types=None,
               row_group_size=1048576,
               obs=None,
               alt_table_name=None,
               batched=True,
               use_duckdb=True):
    
    pg_to_pq(table_name, schema, user=wrds_id, 
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
