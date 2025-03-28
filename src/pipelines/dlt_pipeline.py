import os
import json
from datetime import datetime
import logging
import dlt
import pendulum
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from src.sources.api_source import fetch_data_from_api, load_api_config, get_api_resource
from src.sources.database_source import fetch_data_from_database, load_db_config
from src.sources.storage_source import fetch_data_from_s3
from src.db.duckdb_connection import execute_query
from config.slack_config import load_slack_config

load_slack_config()

def send_slack_message(message: str):
    """Sends a Slack message using the SLACK_WEBHOOK_URL environment variable."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("SLACK_WEBHOOK_URL is not set. Skipping Slack notification.")
        return
    payload = {"text": message}
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code != 200:
            logging.error("Failed to send Slack message: %s", response.text)
        else:
            logging.info("Slack message sent successfully.")
    except Exception as e:
        logging.error("Exception sending Slack message: %s", str(e))


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
        full_env_key = f"{pipeline_name.upper()}__DESTINATION__SNOWFLAKE__CREDENTIALS__{env_key}"
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
        send_slack_message(f"Pipeline `{pipeline_name}` failed: No source URL found.")
        return None

    source_url = result[0][0].strip()
    source_url_lower = source_url.lower()
    logging.info(f"Normalized source URL: {source_url_lower}")

    # If the source URL is an API endpoint (http), load API configuration
    if source_url_lower.startswith("http"):
        # For API sources, load the API config.
        api_config = load_api_config(pipeline_name)
        incremental_type = api_config.get("incremental_type", "FULL").upper()
        if incremental_type == "INCREMENTAL":
            # Build the resource with incremental hints using get_api_resource.
            data_resource = get_api_resource(pipeline_name, table_name, source_url)
        else:
            @dlt.resource(name=table_name, write_disposition="replace")
            def api_data_resource():
                data = fetch_data_from_api(source_url, pipeline_name)
                yield from data
            data_resource = api_data_resource

        data_to_run = data_resource
    elif source_url_lower.startswith("s3://"):
        data = fetch_data_from_s3(pipeline_name)
        data_to_run = data
    elif source_url_lower.startswith(
        ("postgres", "mysql", "bigquery", "redshift", "mssql", "microsoft_sqlserver", "oracle")
    ):
        data_to_run = fetch_data_from_database(pipeline_name)
    else:
        logging.error(f"Unsupported source type for URL: {source_url}")
        return None

    if not data_to_run:
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "No data fetched")
        return None

    # Determine write disposition based on config incremental_type:
    # For database sources the configuration is loaded from the db config.
    # For API sources, we already set the resource with the proper disposition.
    if source_url_lower.startswith("http"):
        write_disposition = None  # Already set in the resource function.
    else:
        db_config = load_db_config(pipeline_name)
        incremental_type = db_config.get("incremental_type", "FULL").upper()
        if incremental_type == "FULL":
            write_disposition = "replace"
        else:  # "INCREMENTAL"
            write_disposition = "merge"

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="snowflake",
        dataset_name=dataset_name
    )
    try:
        start_time = datetime.now()
        log_pipeline_execution(
            pipeline_name, table_name, dataset_name, source_url,
            "started", "Pipeline execution started", start_time=start_time
        )
        send_slack_message(f"Pipeline `{pipeline_name}` started at {start_time.isoformat()}.")

        # Pass write_disposition when running the pipeline if applicable.
        if write_disposition:
            pipeline.run(data_to_run, write_disposition=write_disposition)
        else:
            pipeline.run(data_to_run)

        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(), 2)
        trace = pipeline.last_trace
        row_counts = trace.last_normalize_info.row_counts if trace and trace.last_normalize_info else {}
        total_rows = sum(count for table, count in row_counts.items() if not table.startswith("_dlt_"))
        logging.info(f"Pipeline `{pipeline_name}` completed in {duration} seconds! Rows Loaded: {total_rows}")

        log_pipeline_execution(
            pipeline_name, table_name, dataset_name, source_url,
            "completed", f"Completed in {duration} seconds. Rows Loaded: {total_rows}",
            start_time=start_time, end_time=end_time, trace=trace
        )
        send_slack_message(f"Pipeline `{pipeline_name}` completed in {duration} seconds. Rows Loaded: {total_rows}.")
        return total_rows
    except Exception as e:
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(), 2)
        logging.error(f"Pipeline execution failed in {duration} seconds: {str(e)}")
        trace_obj = pipeline.last_trace if hasattr(pipeline, "last_trace") else None
        log_pipeline_execution(
            pipeline_name, table_name, dataset_name, source_url,
            "error", f"Failed in {duration} seconds: {str(e)}",
            start_time=start_time, end_time=end_time, trace=trace_obj
        )
        send_slack_message(f"Pipeline `{pipeline_name}` failed after {duration} seconds: {str(e)}")
        return None


def log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, event, message, start_time=None, end_time=None, trace=None):
    result = execute_query("SELECT id FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    if not result:
        logging.error(f"No matching pipeline ID found for `{pipeline_name}`. Log entry skipped.")
        return
    pipeline_id = result[0][0]
    log_result = execute_query("SELECT MAX(id) FROM pipeline_logs", fetch=True)
    next_id = (log_result[0][0] + 1) if log_result and log_result[0][0] else 1

    # Compute overall duration if start and end times are provided.
    duration_val = None
    if start_time and end_time:
        duration_val = (end_time - start_time).total_seconds()

    row_counts = None
    trace_json = None
    extract_info = normalize_info = load_info = "N/A"

    if trace:
        try:
            extract_info = str(trace.last_extract_info) if trace.last_extract_info else "N/A"
            normalize_info = str(trace.last_normalize_info) if trace.last_normalize_info else "N/A"
            load_info = str(trace.last_load_info) if trace.last_load_info else "N/A"
            row_counts_dict = trace.last_normalize_info.row_counts if trace.last_normalize_info else {}
            row_counts = json.dumps(row_counts_dict)
            trace_json = str(trace)
        except Exception as e:
            logging.warning(f"Failed to extract trace info: {e}")

    extended_message = (
        f"{message}\nExtract Info: {extract_info}\nNormalize Info: {normalize_info}\nLoad Info: {load_info}"
    )

    query = """
    INSERT INTO pipeline_logs (
        id, pipeline_id, pipeline_name, source_url, snowflake_target, dataset_name,
        event, log_message, duration, start_time, end_time, row_counts, full_trace_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute_query(query, (
        next_id, pipeline_id, pipeline_name, source_url, table_name, dataset_name,
        event, extended_message, duration_val,
        start_time.isoformat() if start_time else None,
        end_time.isoformat() if end_time else None,
        row_counts, trace_json
    ))
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
