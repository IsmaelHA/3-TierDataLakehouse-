from ducklake_utils import connect_ducklake, close_ducklake, extract_date_from_url, BRONZE_MITMA_TABLE,SILVER_MITMA_TABLE,GOLD_MITMA_TABLE
import re
import datetime
import pandas as pd
import holidays

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

def get_day_type(con, date_obj):
    """
    Calculates the day_type logic once for the specific date.
    Returns an Integer.
    """
    target_date_str = date_obj.strftime('%Y-%m-%d')
    
    # 1. Check if it is a Holiday in the DB
    is_holiday = con.execute(
        f"SELECT count(*) FROM ref_holidays WHERE date = '{target_date_str}'"
    ).fetchone()[0] > 0
    
    if is_holiday:
        return 8  # Holiday Type
    
    # 2. If not holiday, check weekday
    wd = date_obj.weekday()
    
    if wd == 6: # Sunday
        return 0
    elif wd == 0: # Monday
        return 1
    elif wd in [1, 2, 3]: # Tue, Wed, Thu
        return 2
    elif wd == 4: # Friday
        return 5
    elif wd == 5: # Saturday
        return 6
    
    return -1 # Should not happen
def create_silver_mitma_table(con):
    con.execute(f"""
            CREATE TABLE IF NOT EXISTS {SILVER_MITMA_TABLE} (
                date DATE,
                hour_period INTEGER,
                origin_zone VARCHAR,
                destination_zone VARCHAR,
                trips DOUBLE,
                day_type INTEGER
            );
        """)
def transform_mitma_silver(con,url:str):
    try:    
        date_obj = extract_date_from_url(url)            
        if not date_obj:
            print(f"⚠️ Could not extract date from {url}. Skipping.")
            return

        target_date = date_obj.strftime('%Y-%m-%d')

        target_date_raw = date_obj.strftime('%Y%m%d')
        target_date_iso = date_obj.strftime('%Y-%m-%d')
        day_type_constant = get_day_type(con, date_obj)
        print(f"Date {target_date} determined as day_type: {day_type_constant}")
        con.execute(f"DELETE FROM {SILVER_MITMA_TABLE} WHERE date = '{target_date_iso}'")                
        
        check_count = con.execute(f"""
            SELECT COUNT(*) FROM {BRONZE_MITMA_TABLE} 
            WHERE CAST(date AS VARCHAR) = '{target_date_raw}'
        """).fetchone()[0]
        
        if check_count == 0:
            print(f"⚠️ WARNING: No rows found in Bronze for raw date '{target_date_raw}'. Skipping Insert.")
            return
        print(f"Found {check_count} rows in Bronze. Proceeding with transformation...")
        con.execute(f"""
        INSERT INTO {SILVER_MITMA_TABLE} 
        SELECT
            strptime(CAST(date AS VARCHAR), '%Y%m%d')::DATE AS date,
            TRY_CAST(hour_period AS INTEGER) AS hour_period,
            REPLACE(REPLACE(origin_zone, '_AM', ''), '_AD', '') AS origin_zone,
            REPLACE(REPLACE(destination_zone, '_AM', ''), '_AD', '') AS destination_zone,
            TRY_CAST(trips AS DOUBLE) AS trips,
            {day_type_constant} AS day_type
        FROM {BRONZE_MITMA_TABLE}
        WHERE
            CAST(date AS VARCHAR) = '{target_date_raw}'
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
        
        total_count = con.execute(f"SELECT COUNT(*) FROM {SILVER_MITMA_TABLE};").fetchone()[0]
        print(f"✅ Success: Silver Layer updated. Total rows in '{SILVER_MITMA_TABLE}': {total_count}")
        # Remove the outliers 
        #con.execute(f
        """
            DELETE FROM {GOLD_MITMA_TABLE}
            WHERE (day_type, hour_period, origin_zone, destination_zone) IN (
                SELECT b.day_type, b.hour_period, b.origin_zone, b.destination_zone
                FROM {GOLD_MITMA_TABLE} b
                WHERE b.trips NOT BETWEEN (s.mean_trips - 10 * s.std_trips) 
                                    AND (s.mean_trips + 10 * s.std_trips)
            );
        """
        #)
        print(" Data quality clean and fixes applied successfully.")
    except Exception as e:
        print(f"Error during DQ process: {e}")
        raise e
        