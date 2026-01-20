def ingestion_bronze_population(
    csv_path: str,
    target_year: int,
    source_year: int | None = None,
    overwrite: bool = False,
):
    from ducklake_utils import connect_ducklake, close_ducklake, table_exists
    con = None
    try:
        con = connect_ducklake()
        # "target_year" controla el nombre de la tabla bronze (bronze_population_<target_year>)
        # "source_year" controla qué registros se filtran del CSV (Periodo termina en <source_year>)
        if source_year is None:
            source_year = target_year

        table_name = f"bronze_population_{target_year}"

        # Skip si ya existe (salvo overwrite=True)
        if (not overwrite) and table_exists(con, table_name):
            print(f"{table_name} already exists, skipping")
            return

        # Create bronze table filtered by source_year
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_csv('{csv_path}', ignore_errors=true)
            WHERE right(CAST("Periodo" AS VARCHAR), 4) = '{source_year}'
        """)

        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if source_year != target_year:
            print(
                f"Bronze population loaded: {count} rows (source_year={source_year} -> table={table_name})"
            )
        else:
            print(f"Bronze population {target_year} loaded: {count} rows")

    finally:
        if con:
            close_ducklake(con)

