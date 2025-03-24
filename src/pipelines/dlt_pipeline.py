# dlt_pipeline.py
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

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_config_path(filename):
    """Returns the correct config file path based on environment (Docker vs. local)."""
    running_in_docker = os.getenv("RUNNING_IN_DOCKER", "").strip().lower()
    local_path = os.path.abspath(os.path.join(os.getcwd(), "config", filename))
    docker_path = f"/app/config/{filename}"

    logging.info(f"🔍 RUNNING_IN_DOCKER env var: {running_in_docker}")
    logging.info(f"🔍 Checking if running in Docker: {running_in_docker == 'true'}")

    return docker_path if running_in_docker == "true" else local_path

def load_snowflake_credentials():
    """Loads Snowflake credentials either from Streamlit session state or from the config file.
       If using key pair authentication, embeds the private key content.
    """
    # Try to load credentials from Streamlit session state
    try:
        import streamlit as st
        if "snowflake_creds" in st.session_state:
            creds = st.session_state.snowflake_creds
            logging.info("🔍 Using Snowflake credentials from session state.")
        else:
            raise KeyError("No session credentials")
    except Exception:
        # Fallback: load from static file
        config_path = get_config_path("snowflake_config.json")
        if not os.path.exists(config_path):
            logging.error(f"❌ Snowflake config file missing! Expected at: {config_path}")
            return {}
        with open(config_path, "r") as f:
            creds = json.load(f)
        logging.info(f"🔍 Loaded Snowflake credentials from file: { {k: v if k != 'private_key' else 'HIDDEN' for k, v in creds.items()} }")
    
    # If using key pair authentication, ensure the private key is properly loaded.
    if creds.get("authenticator") == "snowflake_jwt":
        # If the key was provided via session state, it is already in memory.
        if "private_key" in creds and creds["private_key"].strip().startswith("-----BEGIN"):
            logging.info("🔑 Private key provided in credentials (session or file).")
        else:
            # Otherwise, load from the file path (only when not provided by user)
            original_key_path = creds.get("private_key_path", "/app/config/rsa_key.p8")
            corrected_key_path = get_config_path(os.path.basename(original_key_path))
            creds["private_key_path"] = corrected_key_path

            logging.info(f"🔑 Updated Private key path: {corrected_key_path}")
            if not os.path.exists(corrected_key_path):
                logging.error(f"❌ Private key file missing at {corrected_key_path}!")
                return {}

            try:
                with open(corrected_key_path, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None,
                        backend=default_backend()
                    )
                    creds["private_key"] = private_key.private_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    ).decode("utf-8")
                    logging.info("✅ Successfully loaded and embedded private key into credentials.")
            except Exception as e:
                logging.error(f"❌ Failed to load private key: {e}")
                return {}

    return creds

def set_env_vars(creds, pipeline_name):
    """Sets Snowflake credentials as environment variables dynamically."""
    if not creds:
        logging.error("❌ Missing or invalid Snowflake credentials!")
        return

    env_prefix = pipeline_name.upper()
    mappings = {
        "ACCOUNT": "account",
        "USERNAME": "username",
        "ROLE": "role",
        "DATABASE": "database",
        "SCHEMA": "schema",
        "HOST": "host",
        "AUTHENTICATOR": "authenticator",
        "SESSION_KEEP_ALIVE": "session_keep_alive",
        "PRIVATE_KEY": "private_key"
    }

    for env_key, cred_key in mappings.items():
        value = creds.get(cred_key, "")
        if isinstance(value, bool):
            value = str(value).lower()
        if value:
            os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__{env_key}"] = value

    if creds.get("private_key"):
        logging.info("🔑 Private key successfully set in environment variables.")
    else:
        logging.warning("⚠️ Private key not found in credentials.")

def log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, event, message, duration=None):
    """Logs pipeline execution details into `pipeline_logs` in DuckDB."""
    result = execute_query("SELECT id FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    if not result:
        logging.error(f"❌ No matching pipeline ID found for `{pipeline_name}`. Log entry skipped.")
        return

    pipeline_id = result[0][0]
    log_result = execute_query("SELECT MAX(id) FROM pipeline_logs", fetch=True)
    next_id = (log_result[0][0] + 1) if log_result and log_result[0][0] else 1

    query = """
    INSERT INTO pipeline_logs (id, pipeline_id, pipeline_name, source_url, snowflake_target, dataset_name, event, log_message, duration)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute_query(query, (next_id, pipeline_id, pipeline_name, source_url, table_name, dataset_name, event, message, duration))
    logging.info(f"📝 Logged event `{event}` for pipeline `{pipeline_name}` (ID: {pipeline_id})")

def run_pipeline(pipeline_name: str, dataset_name: str, table_name: str):
    """Runs the DLT pipeline dynamically for the specified source and logs execution."""
    result = execute_query("SELECT source_url FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    if not result:
        logging.error(f"❌ No source URL found for pipeline `{pipeline_name}`")
        return None

    source_url = result[0][0]

    # Fetch data based on the source URL
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
        logging.error(f"❌ Unsupported source type for URL: {source_url}")
        return None

    if not data:
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "No data fetched")
        return None

    creds = load_snowflake_credentials()
    if not creds:
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "Missing Snowflake credentials")
        return None

    set_env_vars(creds, pipeline_name)
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

        logging.info(f"✅ Pipeline `{pipeline_name}` completed in {duration} seconds! Rows Loaded: {total_rows}")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "completed", f"Completed in {duration} seconds. Rows Loaded: {total_rows}", duration)
        return total_rows

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        logging.error(f"❌ Pipeline execution failed in {duration} seconds: {str(e)}")
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", f"Failed in {duration} seconds: {str(e)}", duration)
        return None
