def export_to_csv(con, table_name, output_path):
    con.execute(f"COPY {table_name} TO '{output_path}/{table_name}.csv' (FORMAT CSV, HEADER)")