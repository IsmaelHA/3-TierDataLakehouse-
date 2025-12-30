import os
import shutil
import uuid
from mitma.ducklake_utils import connect_ducklake, close_ducklake

def run_silver_ingestion_atomic(base_path="data/silver_mobility", batch_size=5):
    con = None
    silver_view = "silver_mobility_trips"
    source_table = "stg_mobility_clean_check"
    
    try:
        con = connect_ducklake()
        
        # 1. Check if Source has data
        # We need to get the list of unique dates to batch them
        try:
            dates_result = con.execute(f"SELECT DISTINCT date FROM {source_table} ORDER BY date").fetchall()
            unique_dates = [row[0] for row in dates_result] # List of date objects
        except Exception:
            print(f"Table {source_table} not found or empty. Skipping Silver ingestion.")
            return

        if not unique_dates:
            print("No data in staging. Skipping.")
            return

        total_dates = len(unique_dates)
        print(f"Staging contains data for {total_dates} days. Starting batched ingestion...")

        # 2. BATCH LOOP
        for i in range(0, total_dates, batch_size):
            # Get current batch of dates
            batch_dates = unique_dates[i : i + batch_size]
            
            # Format dates for SQL: '2023-01-01', '2023-01-02'
            sql_date_str = ", ".join([f"'{d.strftime('%Y-%m-%d')}'" for d in batch_dates])
            
            # Create a unique temp folder for THIS batch
            batch_id = str(uuid.uuid4())[:8]
            temp_path = os.path.join(os.path.dirname(base_path), f"_tmp_ingest_{batch_id}")
            
            print(f"--- Processing Batch {i//batch_size + 1}: {len(batch_dates)} dates ---")

            # 3. WRITE BATCH TO TEMP (Safe Zone)
            # We filter the COPY command to only include the dates in this batch
            con.execute(f"""
                COPY (
                    SELECT * FROM {source_table} 
                    WHERE date IN ({sql_date_str})
                ) 
                TO '{temp_path}' 
                (FORMAT PARQUET, PARTITION_BY (date), COMPRESSION 'ZSTD')
            """)
            
            # 4. ATOMIC SWAP FOR THIS BATCH
            # We assume the base_path exists
            os.makedirs(base_path, exist_ok=True)
            
            written_partitions = [d for d in os.listdir(temp_path) if d.startswith("date=")]
            
            for partition_name in written_partitions:
                src_partition = os.path.join(temp_path, partition_name)
                dst_partition = os.path.join(base_path, partition_name)
                
                # A. Delete old data for this specific date
                if os.path.exists(dst_partition):
                    shutil.rmtree(dst_partition)
                
                # B. Move new data in
                shutil.move(src_partition, dst_partition)
                # print(f"   Updated partition: {partition_name}")

            # 5. CLEANUP TEMP FOR THIS BATCH
            # Critical to free up disk space immediately after each batch
            if os.path.exists(temp_path):
                shutil.rmtree(temp_path)

        # 6. FINAL CLEANUP & REFRESH VIEW
        # After all batches are done, refresh the view and drop the staging table
        con.execute(f"""
            CREATE OR REPLACE VIEW {silver_view} AS 
            SELECT * FROM parquet_scan('{base_path}/**/*.parquet', HIVE_PARTITIONING=1);
        """)
        
        # Only drop source if everything succeeded
        con.execute(f"DROP TABLE IF EXISTS {source_table};")
        
        #total_count = con.execute(f"SELECT COUNT(*) FROM {silver_view};").fetchone()[0]
        #print(f"✅ Success: Silver Layer updated. Total rows in '{silver_view}': {total_count}")

    except Exception as e:
        print(f"❌ Silver Partitioning Failed: {e}")
        raise e
    finally:
        # Final cleanup safety net (in case a batch crashed midway)
        # We look for any folders starting with _tmp_ingest_ and remove them
        try:
            parent_dir = os.path.dirname(base_path)
            for d in os.listdir(parent_dir):
                if d.startswith("_tmp_ingest_"):
                    shutil.rmtree(os.path.join(parent_dir, d))
        except Exception:
            pass # Ignore errors during cleanup
            
        if con:
            close_ducklake(con)