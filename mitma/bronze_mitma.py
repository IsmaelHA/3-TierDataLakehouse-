from ducklake_utils import  extract_date_from_url,BRONZE_MITMA_TABLE

def create_bronze_mitma_table(con):
    """
    Creates the table in Neon/S3 if it does not exist.
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_MITMA_TABLE} (
            date VARCHAR,
            hour_period VARCHAR,
            origin_zone VARCHAR,
            destination_zone VARCHAR,
            distance_range VARCHAR,
            origin_activity VARCHAR,
            destination_activity VARCHAR,
            is_origin_study_possible VARCHAR,
            is_destination_study_possible VARCHAR,
            residence_province_code VARCHAR,
            income_range VARCHAR,
            age_group VARCHAR,
            gender VARCHAR,
            trips VARCHAR,
            trips_km_product VARCHAR,
            ingestion_date TIMESTAMP
        );
    """)
    print("✅ Table bronze_mobility_trips checked/created.")

def ingestion_bronze_mitma(con, url:str):
    try:    
        date_obj = extract_date_from_url(url)            
        if not date_obj:
            print(f"⚠️ Could not extract date from {url}. Skipping.")
            return

        target_date = date_obj.strftime('%Y-%m-%d')
                        
        check_query = f"SELECT count(*) FROM bronze_mobility_trips WHERE date = '{target_date}'"

        try:
            count = con.execute(check_query).fetchone()[0]
            if count > 0:
                print(f"⏭️ Skipping {target_date}: Data already found in table.")
                return
        except:
            # Table might not exist yet, so we proceed to insert
            pass

        con.execute(f"""
            INSERT INTO bronze_mobility_trips
            SELECT 
                fecha AS date,
                periodo AS hour_period,
                origen AS origin_zone,
                destino AS destination_zone,
                distancia AS distance_range,
                actividad_origen AS origin_activity,
                actividad_destino AS destination_activity,
                estudio_origen_posible AS is_origin_study_possible,
                estudio_destino_posible AS is_destination_study_possible,
                residencia AS residence_province_code,
                renta AS income_range,
                edad AS age_group,
                sexo AS gender,
                viajes AS trips,
                viajes_km AS trips_km_product,
                CURRENT_TIMESTAMP AS ingestion_date
            FROM read_csv_auto('{url}', compression='gzip', ignore_errors=true, all_varchar=true)
        """)
        
        print(f"Successfully inserted data for {target_date}")            
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        # Cleanup: If a temp folder was left behind due to crash, you might want to log it
        # or attempt to delete it here, though it's safer to leave it for inspection.
        raise e