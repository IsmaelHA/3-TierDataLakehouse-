import duckdb

def test_silver_population():
    con = None
    try:
        con = duckdb.connect()

        # Path to the silver table parquet files
        parquet_path = "C:/Users/34666/Desktop/proyecto/include/my_ducklake.ducklake.files/main/silver_population/*.parquet"

        total = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        print(f"Total rows: {total}")

        by_year = con.execute(f"""
            SELECT year, COUNT(*) AS count
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
                year,
                population
            FROM '{parquet_path}'
            LIMIT 5
        """).fetchall()
        print("\nSample rows:")
        for row in sample:
            print(f"  {row}")

        # Basic data quality checks
        nulls = con.execute(f"""
            SELECT COUNT(*)
            FROM '{parquet_path}'
            WHERE municipality_code IS NULL
               OR year IS NULL
               OR population IS NULL
        """).fetchone()[0]
        print(f"\nNULLs in key fields: {nulls}/{total}")

        negatives = con.execute(f"""
            SELECT COUNT(*)
            FROM '{parquet_path}'
            WHERE population < 0
        """).fetchone()[0]
        print(f"Negative population: {negatives}/{total}")

        duplicates = con.execute(f"""
            SELECT COUNT(*)
            FROM (
                SELECT municipality_code, year, COUNT(*) AS n
                FROM '{parquet_path}'
                GROUP BY municipality_code, year
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        print(f"Duplicate keys (municipality_code,year): {duplicates}")

    finally:
        if con:
            con.close()

if __name__ == "__main__":
    test_silver_population()
