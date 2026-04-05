from .select_sql import qident

def copy_wrds_select_to_pg_table(
            wrds_conn,
            pg_conn,
            select_sql,
            dst_schema,
            dst_table,
            cols,
            uri,
        ):
    
    # 4) COPY with explicit, quoted column list
    cols_csv = ", ".join(qident(pg_conn, c) for c in cols)
    qschema = qident(pg_conn, dst_schema)      
    qtable  = qident(pg_conn, dst_table)

    with wrds_conn.cursor().copy(
        f"COPY ({select_sql}) TO STDOUT (FORMAT BINARY)"
    ) as out:
        with pg_conn.cursor().copy(
            f"COPY {qschema}.{qtable} ({cols_csv}) FROM STDIN (FORMAT BINARY)"
        ) as inn:
            for chunk in out:
                inn.write(chunk)

    return f"{dst_schema}.{dst_table}"
