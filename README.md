# Library to convert WRDS SAS data

This package was created to convert PostgreSQL data to parquet format.
This package has three major functions, one for each of three popular data formats.

 - `wrds_pg_to_pq()`: Exports a WRDS PostgreSQL table to a parquet file.
 - `db_to_pq()`: Exports a PostgreSQL table to a parquet file.
 - `db_schema_to_pq()`: Exports a WRDS PostgreSQL schema to parquet files. 
This package was primarily designed to handle WRDS data, but some support is provided for importing a local SAS file (`*.sas7dbat`) into a PostgreSQL database.

### Report bugs
Author: Ian Gow, <iandgow@gmail.com>
