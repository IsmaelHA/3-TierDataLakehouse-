def transform_silver_population(year: int):
    from ducklake_utils import connect_ducklake, close_ducklake
    con = None
    try:
        con = connect_ducklake()

        bronze_table = f"bronze_population_{year}"
        silver_table = "silver_population"

        # Create silver table if it does not exist
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {silver_table} (
                municipality_code VARCHAR,
                year INTEGER,
                population BIGINT
            )
        """)

        # Skip if this year is already loaded
        count = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE year = {year}"
        ).fetchone()[0]
        if count > 0:
            print(f"{silver_table} already has data for {year}, skipping")
            return

        # Aggregate population by municipality and year
        con.execute(f"""
            INSERT INTO {silver_table} (municipality_code, year, population)
            WITH cleaned AS (
                SELECT
                    split_part(Municipios, ' ', 1) AS municipality_code,
                    CAST(right(CAST(Periodo AS VARCHAR), 4) AS INTEGER) AS year,
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
            f"SELECT COUNT(*) FROM {silver_table} WHERE year = {year}"
        ).fetchone()[0]
        print(f"Silver population loaded for {year}: {inserted} rows")

    finally:
        if con:
            close_ducklake(con)
