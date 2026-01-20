def transform_silver_population(year: int, force_year: int | None = None):
    from ducklake_utils import connect_ducklake, close_ducklake
    con = None
    try:
        con = connect_ducklake()

        # "year" controla la tabla bronze a leer (bronze_population_<year>)
        # Si force_year está definido, escribimos en silver con ese año (útil para remaps/backfills)
        bronze_table = f"bronze_population_{year}"
        silver_table = "silver_population"

        load_year = force_year if force_year is not None else year

        # Create silver table if it does not exist
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {silver_table} (
                municipality_code VARCHAR,
                year INTEGER,
                population BIGINT
            )
        """)

        # Skip si ya existe ese año en silver
        count = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE year = {load_year}"
        ).fetchone()[0]
        if count > 0:
            print(f"{silver_table} already has data for {load_year}, skipping")
            return

        year_expr = (
            f"{int(load_year)}"
            if force_year is not None
            else "CAST(right(CAST(Periodo AS VARCHAR), 4) AS INTEGER)"
        )

        con.execute(f"""
            INSERT INTO {silver_table} (municipality_code, year, population)
            WITH cleaned AS (
                SELECT
                    split_part(Municipios, ' ', 1) AS municipality_code,
                    {year_expr} AS year,
                    TRY_CAST(
                        regexp_replace(NULLIF(TRIM(Total), ''), '[^0-9]', '', 'g')
                        AS BIGINT
                    ) AS population,
                    TRIM(Sexo) AS sex,
                    TRIM("Edad (grandes grupos)") AS age_group
                FROM {bronze_table}
                WHERE Total IS NOT NULL
                  AND Total <> ''
                  AND TRIM(Sexo) <> 'Total'
                  AND TRIM("Edad (grandes grupos)") <> 'Todas las edades'
            )
            SELECT
                municipality_code,
                year,
                SUM(population) AS population
            FROM cleaned
            WHERE population IS NOT NULL
              AND municipality_code IS NOT NULL
            GROUP BY municipality_code, year
        """)

        inserted = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE year = {load_year}"
        ).fetchone()[0]
        print(f"Silver population loaded for {load_year}: {inserted} rows")

    finally:
        if con:
            close_ducklake(con)
