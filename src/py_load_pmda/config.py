import os

def load_config():
    return {}

def get_db_connection_details(config):
    return {
        "type": "postgres",
        "host": os.getenv("PMDA_DB_HOST"),
        "port": os.getenv("PMDA_DB_PORT"),
        "user": os.getenv("PMDA_DB_USER"),
        "password": os.getenv("PMDA_DB_PASSWORD"),
        "dbname": os.getenv("PMDA_DB_DBNAME"),
    }
