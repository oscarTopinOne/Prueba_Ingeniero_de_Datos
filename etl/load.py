def load_to_duckdb(con, table_name, df, cluster_columns=None):
    con.register(f'{table_name}_df', df)

    if cluster_columns:
        order_clause = ', '.join(cluster_columns)
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM {table_name}_df
            ORDER BY {order_clause}
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM {table_name}_df
        """)