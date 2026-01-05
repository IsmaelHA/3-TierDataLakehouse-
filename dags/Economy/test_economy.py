import duckdb

def test_silver_economy():
    con = None
    try:
        con = duckdb.connect()

        parquet_path = "C:/Users/34666/Desktop/proyecto/include/my_ducklake.ducklake.files/main/silver_economy_aggregated/*.parquet"

        total = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        print(f"Total rows: {total}")

        by_year = con.execute(f"""
            SELECT year, COUNT(*) as count
            FROM '{parquet_path}'
            GROUP BY year
            ORDER BY year
        """).fetchall()
        print("\nRows by year:")
        for year, count in by_year:
            print(f"  {year}: {count}")

        sample = con.execute(f"""
            SELECT
                municipality_code,
                district_code,
                section_code,
                year,
                avg_income
            FROM '{parquet_path}'
            LIMIT 5
        """).fetchall()
        print("\nSample rows:")
        for row in sample:
            print(f"  {row}")


        nulls = con.execute(f"""
            SELECT COUNT(*)
            FROM '{parquet_path}'
            WHERE municipality_code IS NULL
               OR district_code IS NULL
               OR section_code IS NULL
               OR year IS NULL
               OR avg_income IS NULL
        """).fetchone()[0]
        print(f"\nRows with NULLs in key fields: {nulls}/{total}")

        negatives = con.execute(f"""
            SELECT COUNT(*)
            FROM '{parquet_path}'
            WHERE avg_income < 0
        """).fetchone()[0]
        print(f"Rows with negative income: {negatives}/{total}")

        duplicates = con.execute(f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    municipality_code, district_code, section_code, year,
                    COUNT(*) AS n
                FROM '{parquet_path}'
                GROUP BY 1,2,3,4
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        print(f"Duplicate keys (mun,district,section,year): {duplicates}")

    finally:
        if con:
            con.close()

if __name__ == "__main__":
    test_silver_economy()
