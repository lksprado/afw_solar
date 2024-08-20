from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import csv
import os

class MissingDatesFinder:
    def __init__(self, postgres_conn_id='postgres_dw'):
        # Use PostgresHook to establish the connection
        self.db_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.filepath = '/opt/airflow/data/solar_project'
        
    def get_max_date_from_db(self):
        query = """
            SELECT MAX(substring(sr.filename,21,10) ::TIMESTAMP::DATE) AS DT 
            FROM dw_lcs.solar_raw sr
        """
        max_date_value = self.fetch_max_date(query)
        return datetime.strptime(str(max_date_value), '%Y-%m-%d').date() if max_date_value else None

    def fetch_max_date(self, query):
        # Use the connection to execute the query
        result = self.db_hook.get_first(query)
        return result[0] if result else None

    def get_current_date(self):
        now = datetime.now()
        if now.hour >= 20:
            return now.date()
        else:
            return (now - timedelta(days=1)).date()

    def find_missing_dates(self):
        start_date = self.get_max_date_from_db() + timedelta(days=1)
        current_date = self.get_current_date()
        if start_date and current_date:
            missing_dates = pd.date_range(start_date, current_date, freq='d').strftime('%Y-%m-%d').tolist()
            return missing_dates
        return []

    def make_temp_file(self):
        missing_dates = self.find_missing_dates()
        full_path = os.path.join(self.filepath, 'missing_dates.csv')
        with open(full_path, mode='w') as file:
            writer = csv.writer(file)
            for missing_date in missing_dates:
                writer.writerow([missing_date])

def main():
    try:
        finder = MissingDatesFinder()
        finder.make_temp_file()
    except Exception as e:
        print(f"Failed to find missing dates: {e}")
        
