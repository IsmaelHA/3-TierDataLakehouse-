

from ducklake_utils import SILVER_MITMA_TABLE,GOLD_MITMA_TABLE

#I think it is not needed to create the table because we need to use create or replace in order to compute the whole silver table
def create_gold_mitma_table(con):
    """
    Creates the structure for the Gold Patterns table.
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_MITMA_TABLE} (
            day_type INTEGER,
            hour_period INTEGER,
            origin_zone VARCHAR,
            destination_zone VARCHAR,
            total_trips DOUBLE,
            avg_trips DOUBLE,
            std_trips DOUBLE,
            num_days_observed INTEGER
        );
    """)
    print(f"✅ Table {GOLD_MITMA_TABLE} checked/created.")

def transform_gold_mitma(con):
    """
    Recomputes the 'Typical Day' patterns based on all data available in Silver.
    This creates a full statistical model of mobility.
    """
    print("🥇 Starting Gold Aggregation (Typical Patterns)...")

    # We use CREATE OR REPLACE to fully refresh the stats.
    # Since we are calculating Global Averages/StdDev, a full refresh is 
    # mathematically the safest and usually very fast in DuckDB.
    try:
            
        con.execute(f"""
            CREATE OR REPLACE TABLE gold_draft_stats AS
            SELECT 
                day_type,
                hour_period,
                origin_zone,
                destination_zone,
                
                -- Total volume ever observed for this pattern
                SUM(trips) as total_trips,
                
                -- The "Typical" behavior (Mean)
                AVG(trips) as avg_trips,
                
                -- The Volatility (Standard Deviation)
                -- STDDEV_POP or STDDEV_SAMP (Sample is better for estimation)
                STDDEV_SAMP(trips) as std_trips,
                
                -- How many distinct dates went into this calculation?
                -- Useful to know if a pattern is statistically significant.
                COUNT(DISTINCT date) as num_days_observed
                
            FROM {SILVER_MITMA_TABLE}
            GROUP BY 
                day_type, 
                hour_period, 
                origin_zone, 
                destination_zone;
        """)
        
        


        print("   ↳ Pass 2: Filtering outliers and finalizing Gold...")
    
        con.execute(f"""
            CREATE OR REPLACE TABLE {GOLD_MITMA_TABLE} AS
            SELECT 
                s.day_type,
                s.hour_period,
                s.origin_zone,
                s.destination_zone,
                
                -- Final Aggregations (Clean Data Only)
                SUM(s.trips) as total_trips,
                AVG(s.trips) as avg_trips,
                STDDEV_SAMP(s.trips) as std_trips,
                COUNT(DISTINCT s.date) as num_days_observed
                
            FROM {SILVER_MITMA_TABLE} s
            JOIN gold_draft_stats d 
                ON s.day_type = d.day_type 
                AND s.hour_period = d.hour_period 
                AND s.origin_zone = d.origin_zone 
                AND s.destination_zone = d.destination_zone
                
            WHERE 
                -- THE FILTER: Keep rows within N standard deviations of the raw mean
                -- We handle NULL/Zero std cases to avoid deleting valid single-data points
                d.std_trips IS NULL 
                OR d.std_trips = 0
                OR (s.trips BETWEEN (d.avg_trips - 3 * d.std_trips) AND (d.avg_trips + 3 * d.std_trips))
                
            GROUP BY 
                s.day_type, s.hour_period, s.origin_zone, s.destination_zone;
        """)
        # Optional: Clean up messy NULL std_devs (if only 1 day exists, std_dev is NULL)
        con.execute(f"""
            UPDATE {GOLD_MITMA_TABLE} 
            SET std_trips = 0 
            WHERE std_trips IS NULL;
        """)
        # Cleanup
        con.execute("DROP TABLE IF EXISTS gold_draft_stats")
        
        count = con.execute(f"SELECT COUNT(*) FROM {GOLD_MITMA_TABLE}").fetchone()[0]
        print(f"✅ Gold Patterns updated. Total unique patterns identified: {count}")
    except Exception as e:
        print(f"Transform to Gold Failed: {e}")
        # Cleanup: If a temp folder was left behind due to crash, you might want to log it
        # or attempt to delete it here, though it's safer to leave it for inspection.
        raise e