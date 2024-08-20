import os 

from airflow.providers.postgres.hooks.postgres import PostgresHook

JSON_DIR = "/opt/airflow/data/solar_project/json_files"

def raw_to_processed():
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

# CREATING STAGING TABLES    
    cursor.execute( 
        """
        CREATE TABLE IF NOT EXISTS dw_lcs.solar_summary_stg ( 
            prod_date DATE,
            duration INT,
            total NUMERIC,
            max NUMERIC,
            co2 NUMERIC
        )
        """
    )

    cursor.execute( 
        """
        CREATE TABLE IF NOT EXISTS dw_lcs.solar_energy_stg ( 
            prod_datehour DATE,
            energy_value NUMERIC
        )
        """
    )    

# CREATING FINAL TABLES
    cursor.execute( 
        """
        CREATE TABLE IF NOT EXISTS dw_lcs.solar_summary_processed ( 
            prod_date DATE,
            duration INT,
            total NUMERIC,
            max NUMERIC,
            co2 NUMERIC
        )
        """
    )

    cursor.execute( 
        """
        CREATE TABLE IF NOT EXISTS dw_lcs.solar_energy_processed ( 
            prod_datehour DATE,
            energy_value NUMERIC
        )
        """
    )

    # THE QUERY WILL TRANSFORM THE STAGING RAW FOR STAGING PROCESSED (summary) 
    cursor.execute(
        """
        INSERT INTO dw_lcs.solar_summary_stg (prod_date, duration, total, max, co2)    
            SELECT
                substring(sr.filename, 21, 10) ::TIMESTAMP::DATE AS prod_date,
                (sr.response->>'duration')::INT AS duration,
                (sr.response->>'total')::NUMERIC AS total,
                (sr.response->>'max')::NUMERIC AS max,
                (sr.response->>'co2')::NUMERIC AS co2
            FROM
                dw_lcs.solar_raw_stg sr   
        """
    )

    # THIS QUERY WILL INSERT ONLY NEW DATA FROM THE JSONS RESPONSE (energy)
    cursor.execute(
        """
        INSERT INTO dw_lcs.solar_energy_stg (prod_datehour, energy_value)
        SELECT
            (substring(sr.filename, 21, 10) ::TIMESTAMP::DATE + INTERVAL '1 hour' * (i - 1)) AS prod_datehour,
            energy_value::NUMERIC
        FROM
            dw_lcs.solar_raw_stg sr,
            LATERAL jsonb_array_elements_text(sr.response->'energy') WITH ORDINALITY AS energy_value(energy_value, i)           
        """
    )
    
    # NOW LOADING TO FINAL TABLES AVOIDING DUPLICATES
    cursor.execute( 
        """
        INSERT INTO dw_lcs.solar_summary_processed(prod_date, duration, total, max, co2)
            SELECT 
            stg.*
            FROM dw_lcs.solar_summary_stg stg
            LEFT JOIN dw_lcs.solar_summary_processed proc
            ON stg.prod_date = proc.prod_date 
            WHERE 
            proc.prod_date IS NULL 
        """
    )

    # NOW LOADING TO FINAL TABLES AVOIDING DUPLICATES
    cursor.execute( 
        """
        INSERT INTO dw_lcs.solar_energy_processed(prod_datehour, energy_value)
            SELECT 
            stg.*
            FROM dw_lcs.solar_energy_stg stg
            LEFT JOIN dw_lcs.solar_energy_processed proc
            ON stg.prod_datehour = proc.prod_datehour 
            WHERE 
            proc.prod_datehour IS NULL 
        """
    )

    # Clear staging tables after processing
    cursor.execute("TRUNCATE TABLE dw_lcs.solar_raw_stg")
    cursor.execute("TRUNCATE TABLE dw_lcs.solar_summary_stg")
    cursor.execute("TRUNCATE TABLE dw_lcs.solar_energy_stg")

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()      
    
    # REMOVING JSON FILES
    
    for filename in os.listdir(JSON_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(JSON_DIR, filename)
            os.remove(filepath)
              