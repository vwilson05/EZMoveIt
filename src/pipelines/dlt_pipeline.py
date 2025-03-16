import os
import logging
import json
import dlt
import time 
from src.sources.api_source import fetch_data_from_api
from src.sources.database_source import fetch_data_from_database
from src.sources.storage_source import fetch_data_from_s3
from src.db.duckdb_connection import execute_query

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")  # Ensure correct path

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def load_snowflake_credentials():
    """Loads Snowflake credentials from the JSON config file."""
    config_path = os.path.join(CONFIG_DIR, "snowflake_config.json")
    
    if not os.path.exists(config_path):
        logging.error(f"❌ Snowflake config file missing! Expected at: {config_path}")
        return {}

    with open(config_path, "r") as f:
        return json.load(f)  # ✅ Fixed incorrect f()


def set_env_vars(creds, pipeline_name):
    """Sets Snowflake credentials as environment variables dynamically."""
    if not creds:
        logging.error("❌ Missing or invalid Snowflake credentials!")
        return

    env_prefix = pipeline_name.upper()
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT"] = creds.get("account", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"] = creds.get("username", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"] = creds.get("password", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"] = creds.get("role", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"] = creds.get("database", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"] = creds.get("schema", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"] = creds.get("host", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__AUTHENTICATOR"] = creds.get("authenticator", "")
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__SESSION_KEEP_ALIVE"] = str(creds.get("session_keep_alive", "true")).lower()

def log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, event, message, duration=None):
    """Logs pipeline execution details into `pipeline_logs` in DuckDB, ensuring pipeline_id is set."""
    
    # ✅ Retrieve the pipeline_id from the `pipelines` table
    result = execute_query("SELECT id FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    
    if not result:
        logging.error(f"❌ No matching pipeline ID found for `{pipeline_name}`. Log entry skipped.")
        return  # 🚨 Exit if no matching pipeline found

    pipeline_id = result[0][0]  # ✅ Extract pipeline ID

    # ✅ Fetch the next available ID manually for pipeline_logs
    log_result = execute_query("SELECT MAX(id) FROM pipeline_logs", fetch=True)
    next_id = (log_result[0][0] + 1) if log_result and log_result[0][0] else 1  # Start at 1 if empty

    # ✅ Insert log entry with correct `pipeline_id`
    query = """
    INSERT INTO pipeline_logs (id, pipeline_id, pipeline_name, source_url, snowflake_target, dataset_name, event, log_message, duration)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute_query(query, (next_id, pipeline_id, pipeline_name, source_url, table_name, dataset_name, event, message, duration))

    logging.info(f"📝 Logged event `{event}` for pipeline `{pipeline_name}` (ID: {pipeline_id})")


def run_pipeline(pipeline_name: str, dataset_name: str, table_name: str):
    """Runs the DLT pipeline dynamically for the specified source and logs execution."""

    # ✅ Fetch pipeline source details
    result = execute_query(
        "SELECT source_url FROM pipelines WHERE name = ?", 
        (pipeline_name,), fetch=True
    )

    if not result:
        logging.error(f"❌ No source URL found for pipeline `{pipeline_name}`")
        return None

    source_url = result[0][0]
    data = None

    # ✅ Determine the data source type and fetch data accordingly
    if source_url.startswith("http"):  
        data = fetch_data_from_api(source_url, pipeline_name)

        # ✅ Wrap API data in a named DLT resource
        @dlt.resource(name=table_name, write_disposition="append")
        def api_data_resource():
            yield from data  # ✅ Ensures data is iterable

    elif source_url.startswith("s3://"):
        data = fetch_data_from_s3(pipeline_name)
    elif source_url.startswith(("postgres", "mysql", "bigquery", "redshift", "mssql")):
        data = fetch_data_from_database(pipeline_name)
    else:
        logging.error(f"❌ Unsupported source type for URL: {source_url}")
        return None

    # ✅ Validate fetched data before proceeding
    if not data:
        logging.warning(f"⚠️ No data fetched for pipeline `{pipeline_name}`")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "No data fetched")
        return None

    # ✅ Load Snowflake credentials
    creds = load_snowflake_credentials()
    if not creds:
        logging.error("❌ Missing Snowflake credentials! Aborting pipeline execution.")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "Missing Snowflake credentials")
        return None

    set_env_vars(creds, pipeline_name)  # ✅ Apply Snowflake environment variables

    # ✅ Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="snowflake",
        dataset_name=dataset_name
    )

    try:
        start_time = time.time()  # ✅ Capture start time
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "started", "Pipeline execution started")

        # ✅ If API data, run with the named resource
        if source_url.startswith("http"):
            load_info = pipeline.run(api_data_resource)
        else:
            load_info = pipeline.run(data)

        # ✅ Capture duration and compute execution time
        duration = round(time.time() - start_time, 2)

        # ✅ Check if data was actually loaded
        if pipeline.last_trace and pipeline.last_trace.last_normalize_info:
            row_counts_dict = pipeline.last_trace.last_normalize_info.row_counts
            total_rows = sum(count for table, count in row_counts_dict.items() if not table.startswith("_dlt_"))
            logging.info(f"✅ Pipeline `{pipeline_name}` completed in {duration} seconds! Rows Loaded: {total_rows}")

            log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "completed", f"Completed in {duration} seconds. Rows Loaded: {total_rows}", duration)
            return total_rows

        logging.warning(f"⚠️ Pipeline `{pipeline_name}` ran but no rows were loaded.")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "completed", f"Completed in {duration} seconds. No rows loaded", duration)
        return 0

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        logging.error(f"❌ Pipeline execution failed in {duration} seconds: {str(e)}")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", f"Failed in {duration} seconds: {str(e)}", duration)
        return None