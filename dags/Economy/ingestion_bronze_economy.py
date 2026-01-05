def ingestion_bronze_economy(csv_path: str, year: int):
    from ducklake_utils import connect_ducklake, close_ducklake, table_exists
    con = None
    try:
        con = connect_ducklake()
        table_name = f"bronze_economy_{year}"

        if table_exists(con, table_name):
            print(f"{table_name} already exists, skipping")
            return

        # Creamos bronze por año filtrando por "Periodo"
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_csv('{csv_path}', ignore_errors=true)
            WHERE right(CAST("Periodo" AS VARCHAR), 4) = '{year}'
        """)

        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Bronze economy {year} loaded: {count} rows")

    finally:
        if con:
            close_ducklake(con)

