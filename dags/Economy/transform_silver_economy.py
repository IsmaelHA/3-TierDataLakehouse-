def transform_silver_economy(year: int):
 
    from ducklake_utils import connect_ducklake, close_ducklake
    con = None
    try:
        con = connect_ducklake()

        bronze_table = f"bronze_economy_{year}"
        silver_table = "silver_economy_aggregated"

      
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {silver_table} (
                municipality_code VARCHAR,
                district_code VARCHAR,
                section_code VARCHAR,
                year INTEGER,
                avg_income DOUBLE
            )
        """)

        # Skip transformation if this year is already loaded
        count = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE year = {year}"
        ).fetchone()[0]
        if count > 0:
            print(f"{silver_table} already has data for year {year}, skipping")
            return

        # Transform and aggregate data from bronze to silver
        con.execute(f"""
            INSERT INTO {silver_table}
            WITH cleaned AS (
                SELECT
                    split_part("Municipios", ' ', 1) AS municipality_code,
                    split_part("Distritos", ' ', 1) AS district_code,
                    split_part("Secciones", ' ', 1) AS section_code,
                    CAST(right(CAST("Periodo" AS VARCHAR), 4) AS INTEGER) AS year,
                    NULLIF(TRIM("Total"), '') AS raw_income
                FROM {bronze_table}
            ),
            parsed AS (
                SELECT
                    municipality_code,
                    district_code,
                    section_code,
                    year,
                    TRY_CAST(
                        REPLACE(REPLACE(raw_income, '.', ''), ',', '.') AS DOUBLE
                    ) AS income_value
                FROM cleaned
            )
            SELECT
                municipality_code,
                district_code,
                section_code,
                year,
                AVG(income_value) AS avg_income
            FROM parsed
            WHERE income_value IS NOT NULL
              AND municipality_code IS NOT NULL
              AND district_code IS NOT NULL
              AND section_code IS NOT NULL
            GROUP BY municipality_code, district_code, section_code, year
        """)

        
        inserted = con.execute(
            f"SELECT COUNT(*) FROM {silver_table} WHERE year = {year}"
        ).fetchone()[0]
        print(f"Silver economy loaded for {year}: {inserted} rows")

    finally:
        if con:
            close_ducklake(con)
