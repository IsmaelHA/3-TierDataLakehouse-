from datetime import datetime, timedelta
from airflow.sdk import dag, task,Param
#from airflow.models.param import Param
# --- Import your custom functions ---
# Ensure these modules are in your PYTHONPATH or Airflow plugins folder
from mitma.fetch_url_mitma import fetch_mitma_url
from mitma.bronze_mitma import create_bronze_mitma_table,ingestion_bronze_mitma
from mitma.silver_mitma import transform_mitma_silver,ingest_spain_holidays,create_silver_mitma_table
from mitma.gold_mitma import transform_gold_mitma,create_gold_mitma_table
from mitma.generate_report import generate_mobility_report_s3
from ducklake_utils import connect_ducklake, close_ducklake, extract_date_from_url,DUCKLAKE_DATA_PATH
# --- Default Arguments ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}


@dag(
    dag_id='mitma_mobility_pipeline',
    default_args=default_args,
    description='Pipeline for MITMA Mobility: Fetch -> Bronze -> Quality -> Silver -> Stats',
    start_date=datetime(2023, 1, 1), # Adjust start date
    catchup=False,                   # Set True if you want to backfill past dates
    tags=['mitma', 'mobility'],
    params={
        "start_date": Param(default="None", type=["string", "null"], description="YYYY-MM-DD"),
        "end_date": Param(default=None, type=["string", "null"], description="YYYY-MM-DD"),
    }
)
def mitma_pipeline():

    # 1. TASK: Fetch URLs
    # We use the logical_date (execution date) as the target date
    @task
    def task_fetch_urls(**context):
        con = connect_ducklake()
        # 1. Try to get manual parameters from the UI Trigger
        # "params" dictionary is automatically available in context
        manual_start = context['params'].get('start_date')
        manual_end = context['params'].get('end_date')
        
        # 2. Determine which dates to use
        if manual_start and manual_end:
            print(f"Manual trigger detected: {manual_start} to {manual_end}")
            s_date = manual_start
            e_date = manual_end
        else:
            # Fallback: Use the Airflow execution date (logical_date)
            # Useful if you switch back to a daily schedule
            print(f"No manual dates provided. Using execution date: {context['ds']}")
            s_date = context['ds']
            e_date = context['ds']
        # 3. Call your function
        urls = fetch_mitma_url(s_date, e_date)
        
        if not urls:
            print(f"No URLs found between {s_date} and {e_date}.")
            return []
        close_ducklake(con)
        return urls

    @task
    def task_create_bronze_mitma(): 
        con = connect_ducklake()
        create_bronze_mitma_table(con)
        close_ducklake(con)
        return True
        
    # 2. TASK: Ingest to Bronze
    # This task receives the list of URLs from the previous task via XComs automatically
    @task
    def task_ingest_bronze(url: str):
        con = connect_ducklake()
        if not url:
            print("Skipping ingestion: No URL provided.")
            return
        
        ingestion_bronze_mitma(con,url)
        close_ducklake(con)
        return url


    @task
    def task_create_holidays(urls):
        con = connect_ducklake()
        valid_dates_list = set(extract_date_from_url(url) for url in urls)
        valid_dates_list.discard(None)
        if not valid_dates_list:
            print("No valid dates found.")
            return
        #sql_date_str = ", ".join([f"'{d.strftime('%Y-%m-%d')}'" for d in valid_dates_list])
        # Change '%Y-%m-%d' to '%Y%m%d'
        #ducklake_date_str = ", ".join([f"'{d.strftime('%Y%m%d')}'" for d in valid_dates_list])
        unique_years = {d.year for d in valid_dates_list} # This is a set, so, years are unique
        for year in sorted(unique_years):
            print(f"Ingesting holidays for {year}")
            ingest_spain_holidays(con,year)
        
        close_ducklake(con)
        return True

    @task
    def task_create_silver_table():
        con = connect_ducklake()
        create_silver_mitma_table(con)
        close_ducklake(con)

    # 4. TASK: Silver Transformation
    @task
    def task_silver_transform(url):
        con = connect_ducklake()
        print("Running Silver Ingestion (Atomic Swap)...")
        transform_mitma_silver(con,url)
        close_ducklake(con)
    # 5. TASK: Update Statistics
    """
    @task
    def task_create_gold():
        print("Updating Data Quality Stats...")
        run_stats_update()
    """
    @task
    def task_transform_gold():
        con = connect_ducklake()
        print("Updating Data Quality Stats...")
        transform_gold_mitma(con)
        close_ducklake(con)

    # In your DAG
    @task
    def task_create_report():
        con = connect_ducklake()
        try:
            # Generate and Upload
            generate_mobility_report_s3(
                con=con, 
                bucket_name=DUCKLAKE_DATA_PATH, 
                s3_key="reports/mitma_mobility.pdf"
            )
        finally:
            close_ducklake(con)
    # --- Define the Flow / Dependencies ---
    


    # MAIN PIPELINE
    url_list = task_fetch_urls()
    task_create_holidays(url_list)

    ingested_results = task_ingest_bronze.expand(url=url_list)
    task_create_bronze_mitma() >> ingested_results

    silver_results = task_silver_transform.expand(url=ingested_results)
    task_create_silver_table() >> silver_results
    silver_results >> task_transform_gold()#>>task_create_report()


# Instantiate the DAG
dag_instance = mitma_pipeline()