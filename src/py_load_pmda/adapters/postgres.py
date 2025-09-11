import psycopg2
from io import StringIO
import csv
import pandas as pd

class PostgreSQLAdapter:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self, connection_details):
        self.conn = psycopg2.connect(
            dbname=connection_details["dbname"],
            user=connection_details["user"],
            password=connection_details["password"],
            host=connection_details["host"],
            port=connection_details["port"],
        )
        self.cursor = self.conn.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def ensure_schema(self, schema_def):
        schema_name = schema_def["schema_name"]
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        for table_name, table_def in schema_def["tables"].items():
            columns = ", ".join([f"{col_name} {col_type}" for col_name, col_type in table_def["columns"].items()])
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns});")

    def bulk_load(self, data, target_table, schema, mode):
        if mode == "overwrite":
            self.cursor.execute(f"TRUNCATE TABLE {schema}.{target_table};")

        cols = ','.join(data.columns)
        sio = StringIO()
        writer = csv.writer(sio)
        for row in data.itertuples(index=False, name=None):
            writer.writerow([None if pd.isna(val) else val for val in row])
        sio.seek(0)

        self.cursor.copy_expert(f"COPY {schema}.{target_table} ({cols}) FROM STDIN WITH CSV", sio)


    def get_all_states(self, schema):
        return []
