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

def get_table_column_types(conn, schema: str, table: str) -> dict[str, str]:
    sql = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return {name: dtype for name, dtype in cur.fetchall()}


def get_table_numeric_bounds(
    conn,
    schema: str,
    table: str,
) -> dict[str, tuple[int, int]]:
    sql = """
    SELECT
        column_name,
        numeric_precision,
        numeric_scale
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
      AND data_type = 'numeric'
      AND numeric_precision IS NOT NULL
      AND numeric_scale IS NOT NULL
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return {
            name: (int(precision), int(scale))
            for name, precision, scale in cur.fetchall()
        }
