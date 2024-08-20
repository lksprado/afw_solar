import os
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Directory containing JSON files
JSON_DIR = "/opt/airflow/data/solar_project/json_files"


def load_json_to_postgres():
    # CONNECT TO DB
    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # CREATE SCHEMA IF NOT EXISTS
    cursor.execute(
        """
        CREATE SCHEMA IF NOT EXISTS dw_lcs
        """
    )
    # CREATE STAGING TABLE IF NOT EXISTS
    cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS dw_lcs.solar_raw_stg (
        filename VARCHAR(50),
        response JSONB,
        PRIMARY KEY (filename)
        )
    """
    )
    
    # UPLOAD JSONS TO STAGING 
    for filename in os.listdir(JSON_DIR):
        if filename.endswith(".json"):
            file_path = os.path.join(JSON_DIR, filename)
            with open(file_path, "r") as file:
                response = json.load(file)
                cursor.execute(
                    "SELECT 1 FROM dw_lcs.solar_raw_stg WHERE filename = %s", (filename,)
                )
                if cursor.fetchone() is None:
                    cursor.execute(
                        "INSERT INTO dw_lcs.solar_raw_stg (filename, response) VALUES (%s, %s)",
                        (filename, json.dumps(response)),
                    )

    # MOVE NEW DATA ONLY TO FINAL TABLE
    cursor.execute(
        """
        INSERT INTO dw_lcs.solar_raw (filename, response)
        SELECT s.filename, s.response
        FROM dw_lcs.solar_raw_stg s
        LEFT JOIN dw_lcs.solar_raw r ON s.filename = r.filename
        WHERE r.filename IS NULL
    """
    )


    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()
