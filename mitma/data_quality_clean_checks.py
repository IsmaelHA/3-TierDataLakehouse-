from mitma.ducklake_utils import connect_ducklake, close_ducklake, extract_date_from_url
import re
import datetime
import pandas as pd
import holidays
from mitma.transform_silver_mitma import run_silver_ingestion_atomic
BRONZE_PATH = "data/bronze_mobility" # Path where your parquet files are


def ensure_bronze_view_exists(con):
    """
    CRITICAL STEP: This tells DuckDB where to look for the raw data.
    We re-run this every time just to be safe (it's free/fast).
    """
    con.execute(f"""
        CREATE OR REPLACE VIEW bronze_raw_mobility_trips AS 
        SELECT * FROM parquet_scan('{BRONZE_PATH}/**/*.parquet', HIVE_PARTITIONING=1);
    """)

def ingest_spain_holidays(con,year=2023):

    # 1. Create the table structure if it doesn't exist yet
    con.execute("""
        CREATE TABLE IF NOT EXISTS ref_holidays (
            date DATE,
            is_holiday BOOLEAN
        );
    """)

    # 2. Check if the year is already inserted
    existing_count = con.execute(
        f"SELECT count(*) FROM ref_holidays WHERE year(date) = {year};"
    ).fetchone()[0]

    if existing_count > 0:
        print(f"⚠️  Skipping {year}: Holidays for this year already exist in ref_holidays.")
        return

    # 3. If we are here, the data is missing. Let's fetch it.
    print(f"Fetching Spain holidays for {year}...")
    es_holidays = holidays.country_holidays("ES", years=[year])

    rows = []
    # Note: We are currently ignoring 'name' (e.g., "Christmas"), 
    # but you could add a 'holiday_name' column to the table if you want it for debugging.
    for date, name in es_holidays.items():
        rows.append({
            "date": date,
            "is_holiday": True
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print(f"No holidays found for {year} (Check library configuration).")
        return

    con.register("df_holidays", df)
    
    con.execute("""
        INSERT INTO ref_holidays (date, is_holiday)
        SELECT date, is_holiday FROM df_holidays;
    """)
    con.unregister("df_holidays")
    print(f"Successfully inserted {len(df)} holidays for {year}.")


def update_calendar(con, temp_table_name):
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_calendar_dates (
            date DATE,
            day_of_week INTEGER,
            is_holiday BOOLEAN
        );
    """)
    con.execute(f"""

        -- Step A: Clear out any dates that exist in our incoming batch
        DELETE FROM bronze_calendar_dates 
        WHERE date IN (SELECT DISTINCT date FROM {temp_table_name});

        -- Step B: Insert the fresh calculations for those dates
        INSERT INTO bronze_calendar_dates (date, day_of_week, is_holiday)
        SELECT DISTINCT
            m.date,
            STRFTIME(m.date, '%w')::INTEGER as day_of_week,
            CASE WHEN h.date IS NOT NULL THEN TRUE ELSE FALSE END as is_holiday
        FROM {temp_table_name} m
        LEFT JOIN ref_holidays h ON m.date = h.date;
    """)
    print("Calendar Table updated")
def create_permanent_tables_if_not_exist(table_name,con):
    con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date DATE,
                hour_period INTEGER,
                origin_zone VARCHAR,
                destination_zone VARCHAR,
                trips DOUBLE,
                day_type INTEGER
            );
        """)
def run_data_quality_fixes(valid_urls:list,batch_size=1):
    silver_view = "silver_mobility_trips"
    valid_dates_list = set(extract_date_from_url(url) for url in valid_urls)
    valid_dates_list.discard(None)
    if not valid_dates_list:
        print("No valid dates found.")
        return
    #sql_date_str = ", ".join([f"'{d.strftime('%Y-%m-%d')}'" for d in valid_dates_list])
    # Change '%Y-%m-%d' to '%Y%m%d'
    #ducklake_date_str = ", ".join([f"'{d.strftime('%Y%m%d')}'" for d in valid_dates_list])
    unique_years = {d.year for d in valid_dates_list} # This is a set, so, years are unique
    sorted_dates = sorted(list(valid_dates_list))

    table_name= "stg_mobility_clean_check"
    temp_table_name='batch_mobility_clean'
    con = None
    try:
        con = connect_ducklake()
        ensure_bronze_view_exists(con)

        for year in sorted(unique_years):
            print(f"Ingesting holidays for {year}")
            ingest_spain_holidays(con,year)

        total_dates = len(sorted_dates)
        try:
            processed_dates_query = con.execute(f"""SELECT DISTINCT date FROM {silver_view}""").fetchall()
            processed_dates_set = {row[0] for row in processed_dates_query}
        except Exception as e:
            # Catch-all for other unexpected DB errors
            print(f"⚠️ Unexpected error reading {silver_view}: {e}")
            processed_dates_set = set()

        for i in range(0, total_dates, batch_size):
            # Get the slice of dates for this batch
            batch_dates = sorted_dates[i : i + batch_size]

            dates_to_process = [d for d in batch_dates if d not in processed_dates_set]
            if not dates_to_process:
                print(f"Skipping batch starting {batch_dates[0]}: All dates already processed.")
                continue

            create_permanent_tables_if_not_exist(table_name, con)

            # Create the filter string: '20230101', '20230102', ...
            # Matches the FOLDER names in Bronze
            ducklake_date_str = ", ".join([f"'{d.strftime('%Y%m%d')}'" for d in batch_dates])
            
            # Create the SQL Delete string: '2023-01-01', ...
            # Used to clear the target table before inserting (Idempotency)
            sql_iso_date_str = ", ".join([f"'{d.strftime('%Y-%m-%d')}'" for d in batch_dates])

            print(f"--- Processing Batch {i//batch_size + 1} ({len(batch_dates)} dates) ---")
            print(f"Processing batch for dates: {ducklake_date_str}")
            con.execute("""BEGIN TRANSACTION;""")
            con.execute(f"DELETE FROM {table_name} WHERE date IN ({sql_iso_date_str});")
            con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
            SELECT
                strptime(CAST(date AS VARCHAR), '%Y%m%d')::DATE AS date,
                TRY_CAST(hour_period AS INTEGER) AS hour_period,
                REPLACE(REPLACE(origin_zone, '_AM', ''), '_AD', '') AS origin_zone,
                REPLACE(REPLACE(destination_zone, '_AM', ''), '_AD', '') AS destination_zone,
                TRY_CAST(trips AS DOUBLE) AS trips
            FROM bronze_raw_mobility_trips
            WHERE
                date IN ({ducklake_date_str})
                AND origin_zone NOT LIKE 'PT%'
                AND destination_zone NOT LIKE 'PT%'
                AND origin_zone NOT LIKE 'FR%'
                AND destination_zone NOT LIKE 'FR%'
                AND origin_zone <> 'externo'
                AND destination_zone <> 'externo'
                AND strptime(CAST(date AS VARCHAR), '%Y%m%d') IS NOT NULL
                AND TRY_CAST(trips AS DOUBLE) IS NOT NULL
                AND TRY_CAST(hour_period AS INTEGER) IS NOT NULL;
                """)
            temp_count = con.execute(f"SELECT COUNT(*) FROM {temp_table_name};").fetchone()[0]
            print(f" Rows in temp table '{temp_table_name}': {temp_count}")
            update_calendar(con,temp_table_name)
            #prepare the table for the silver layer
            con.execute(f"""
            INSERT INTO {table_name} 
            SELECT
                t.date,
                t.hour_period,
                t.origin_zone,
                t.destination_zone,
                SUM(t.trips) AS trips,

                CASE
                    WHEN c.is_holiday THEN 8
                    WHEN c.day_of_week IN (2,3,4) THEN 2
                    WHEN c.day_of_week = 6 THEN 6
                    WHEN c.day_of_week = 5 THEN 5
                    WHEN c.day_of_week = 1 THEN 1
                    WHEN c.day_of_week = 0 THEN 0
                END AS day_type

            FROM {temp_table_name} t
            JOIN bronze_calendar_dates c
            ON t.date = c.date
            GROUP BY
                    t.date,
                    t.hour_period,
                    t.origin_zone,
                    t.destination_zone,
                    day_type   ;

            """)
            
            # Remove the outliers 
            #con.execute(f
            """
                DELETE FROM {table_name}
                WHERE (day_type, hour_period, origin_zone, destination_zone) IN (
                    SELECT b.day_type, b.hour_period, b.origin_zone, b.destination_zone
                    FROM {table_name} b
                    JOIN silver_zone_stats s 
                    ON b.origin_zone = s.origin_zone
                    AND b.destination_zone = s.destination_zone 
                    AND b.day_type = s.day_type 
                    AND b.hour_period = s.hour_period
                    WHERE b.trips NOT BETWEEN (s.mean_trips - 10 * s.std_trips) 
                                        AND (s.mean_trips + 10 * s.std_trips)
                );
            """
            #)
            con.execute("""COMMIT;""")
            run_silver_ingestion_atomic(con=con)
        #total_count = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        #print(f"Total rows in '{table_name}': {total_count}")
        print(" Data quality clean and fixes applied successfully.")
    except Exception as e:
        print(f"Error during DQ process: {e}")
        raise e
    finally:
        close_ducklake(con)
        