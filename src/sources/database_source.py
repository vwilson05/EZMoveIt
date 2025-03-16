import logging
import json
import os
import dlt
import duckdb
from src.db.duckdb_connection import execute_query

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")

# ✅ Load Database Config
def load_db_config(pipeline_name):
    config_path = os.path.join(CONFIG_DIR, f"{pipeline_name.replace(' ', '_').lower()}_config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}

# ✅ Fetch Data from SQL Databases (Postgres, MySQL, Redshift, etc.)
def fetch_data_from_database(pipeline_name):
    db_config = load_db_config(pipeline_name)
    if not db_config:
        logging.error(f"❌ No database config found for `{pipeline_name}`!")
        return []

    query = db_config.get("query", "SELECT * FROM my_table LIMIT 100")

    logging.info(f"Running query for `{pipeline_name}`: {query}")
    
    conn = duckdb.connect(database=":memory:")  # Replace with actual DB connection
    try:
        results = conn.execute(query).fetchall()
        conn.close()
        return results
    except Exception as e:
        logging.error(f"❌ Database Query Failed: {e}")
        return []
