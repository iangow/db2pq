def get_table_columns(conn, schema: str, table: str) -> list[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [r[0] for r in cur.fetchall()]
