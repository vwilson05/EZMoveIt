import os
import json
import time
import logging
import dlt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from src.sources.api_source import fetch_data_from_api
from src.sources.database_source import fetch_data_from_database
from src.sources.storage_source import fetch_data_from_s3
from src.db.duckdb_connection import execute_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_config_path(filename):
    running_in_docker = os.getenv("RUNNING_IN_DOCKER", "").strip().lower()
    local_path = os.path.abspath(os.path.join(os.getcwd(), "config", filename))
    docker_path = f"/app/config/{filename}"
    logging.info(f"RUNNING_IN_DOCKER: {running_in_docker}")
    return docker_path if running_in_docker == "true" else local_path

def set_env_vars(creds, pipeline_name):
    """Sets Snowflake credentials as environment variables dynamically."""
    if not creds:
        logging.error("Missing or invalid Snowflake credentials!")
        return

    env_prefix = pipeline_name.upper()

    # For key-pair (JWT) mode, use private_key; for username/password, include warehouse and password.
    if creds.get("authenticator") == "snowflake_jwt":
        mappings = {
            "USERNAME": "username",
            "ROLE": "role",
            "DATABASE": "database",
            "SCHEMA": "",       # Optional; include if needed
            "HOST": "host",
            "WAREHOUSE": "",
            "AUTHENTICATOR": "authenticator",
            "SESSION_KEEP_ALIVE": "session_keep_alive",
            "PRIVATE_KEY": "private_key"
        }
    else:
        # Username/Password mode: do not set an authenticator or private key.
        mappings = {
            "USERNAME": "username",
            "ROLE": "role",
            "DATABASE": "database",
            "SCHEMA": "",    # Omit if not used
            "HOST": "host",
            "WAREHOUSE": "",
            "SESSION_KEEP_ALIVE": "session_keep_alive",
            "PASSWORD": "password"
        }

    for env_key, cred_key in mappings.items():
        value = creds.get(cred_key) or ""
        if isinstance(value, bool):
            value = str(value).lower()
        full_env_key = f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__{env_key}"
        os.environ[full_env_key] = value
        if env_key not in ("PASSWORD", "PRIVATE_KEY"):
            logging.info("Set env var %s: %s", full_env_key, value)
        else:
            logging.info("Set env var %s: [HIDDEN]", full_env_key)

    if creds.get("authenticator") == "snowflake_jwt":
        if creds.get("private_key"):
            logging.info("Private key successfully set.")
        else:
            logging.warning("Private key not found in credentials.")
    else:
        if creds.get("password"):
            logging.info("Password successfully set.")
        else:
            logging.warning("Password not found in credentials.")

def load_snowflake_credentials():
    try:
        import streamlit as st
        if "snowflake_creds" in st.session_state:
            creds = st.session_state.snowflake_creds
            logging.info("Session state credentials: %s", creds)
            return creds
        else:
            logging.error("No Snowflake credentials found in session state!")
            return {}
    except Exception as e:
        logging.error("Error loading credentials: %s", e)
        return {}

def run_pipeline(pipeline_name: str, dataset_name: str, table_name: str):
    result = execute_query("SELECT source_url FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    if not result:
        logging.error(f"No source URL found for pipeline `{pipeline_name}`")
        return None
    source_url = result[0][0]
    if source_url.startswith("http"):
        data = fetch_data_from_api(source_url, pipeline_name)
        @dlt.resource(name=table_name, write_disposition="append")
        def api_data_resource():
            yield from data
    elif source_url.startswith("s3://"):
        data = fetch_data_from_s3(pipeline_name)
    elif source_url.startswith(("postgres", "mysql", "bigquery", "redshift", "mssql")):
        data = fetch_data_from_database(pipeline_name)
    else:
        logging.error(f"Unsupported source type for URL: {source_url}")
        return None
    if not data:
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "No data fetched")
        return None
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="snowflake",
        dataset_name=dataset_name
    )
    try:
        start_time = time.time()
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "started", "Pipeline execution started")
        if source_url.startswith("http"):
            pipeline.run(api_data_resource)
        else:
            pipeline.run(data)
        duration = round(time.time() - start_time, 2)
        row_counts = pipeline.last_trace.last_normalize_info.row_counts if pipeline.last_trace and pipeline.last_trace.last_normalize_info else {}
        total_rows = sum(count for table, count in row_counts.items() if not table.startswith("_dlt_"))
        logging.info(f"Pipeline `{pipeline_name}` completed in {duration} seconds! Rows Loaded: {total_rows}")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "completed", f"Completed in {duration} seconds. Rows Loaded: {total_rows}", duration)
        return total_rows
    except Exception as e:
        duration = round(time.time() - start_time, 2)
        logging.error(f"Pipeline execution failed in {duration} seconds: {str(e)}")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", f"Failed in {duration} seconds: {str(e)}", duration)
        return None

def log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, event, message, duration=None):
    result = execute_query("SELECT id FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    if not result:
        logging.error(f"No matching pipeline ID found for `{pipeline_name}`. Log entry skipped.")
        return
    pipeline_id = result[0][0]
    log_result = execute_query("SELECT MAX(id) FROM pipeline_logs", fetch=True)
    next_id = (log_result[0][0] + 1) if log_result and log_result[0][0] else 1
    query = """
    INSERT INTO pipeline_logs (id, pipeline_id, pipeline_name, source_url, snowflake_target, dataset_name, event, log_message, duration)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute_query(query, (next_id, pipeline_id, pipeline_name, source_url, table_name, dataset_name, event, message, duration))
    logging.info("Logged event `%s` for pipeline `%s` (ID: %s)", event, pipeline_name, pipeline_id)

def run_pipeline_with_creds(pipeline_name: str, dataset_name: str, table_name: str, creds: dict):
    set_env_vars(creds, pipeline_name)
    env_keys = [
        "ACCOUNT", "USERNAME", "DATABASE", "SCHEMA", "HOST", "AUTHENTICATOR", "PASSWORD"
    ]
    for key in env_keys:
        full_key = f"{pipeline_name.upper()}__DESTINATION__SNOWFLAKE__CREDENTIALS__{key}"
        logging.info("Env %s = %s", full_key, os.environ.get(full_key))
    return run_pipeline(pipeline_name, dataset_name, table_name)
