import os
import json
from datetime import datetime
import logging
import dlt
import pendulum
import requests
import time
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from src.sources.api_source import fetch_data_from_api, load_api_config, get_api_resource
from src.sources.database_source import fetch_data_from_database, load_db_config
from src.sources.storage_source import fetch_data_from_s3
from src.db.duckdb_connection import execute_query
from config.slack_config import load_slack_config

load_slack_config()

import os
os.environ["PROGRESS"] = "log" 

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


def log_pipeline_execution(pipeline_name: str, table_name: str, dataset_name: str, source_url: str, event: str, log_message: str, start_time: datetime = None, end_time: datetime = None, trace = None):
    """Log pipeline execution events to the database."""
    try:
        # Get pipeline ID
        pipeline_result = execute_query("SELECT id FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
        if not pipeline_result:
            logging.error(f"No pipeline found with name: {pipeline_name}")
            return
        pipeline_id = pipeline_result[0][0]

        # Get latest run ID
        run_result = execute_query(
            "SELECT id FROM pipeline_runs WHERE pipeline_name = ? ORDER BY start_time DESC LIMIT 1",
            (pipeline_name,),
            fetch=True
        )
        run_id = run_result[0][0] if run_result else None

        # Calculate duration if start and end times are provided
        duration = None
        if start_time and end_time:
            duration = (end_time - start_time).total_seconds()

        # Prepare row counts
        row_counts = None
        if trace and hasattr(trace, "last_normalize_info") and trace.last_normalize_info:
            row_counts = json.dumps(trace.last_normalize_info.row_counts)

        # Log to database
        execute_query(
            """
            INSERT INTO pipeline_logs (
                id, pipeline_id, run_id, event, timestamp, duration,
                log_message, pipeline_name, source_url, target_table,
                dataset_name, stage, start_time, end_time, row_counts,
                full_trace_json
            ) VALUES (
                (SELECT COALESCE(MAX(id), 0) + 1 FROM pipeline_logs),
                ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                pipeline_id, run_id, event, duration, log_message,
                pipeline_name, source_url, table_name, dataset_name,
                event, start_time, end_time, row_counts,
                json.dumps(trace.__dict__) if trace else None
            )
        )
        
        # Log the event
        logging.info("Logged event `%s` for pipeline `%s`", event, pipeline_name)
        
        # Comment out Slack webhook for now
        # webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        # if webhook_url:
        #     try:
        #         payload = {
        #             "text": f"Pipeline Event: {pipeline_name} - {event}",
        #             "blocks": [
        #                 {
        #                     "type": "section",
        #                     "text": {
        #                         "type": "mrkdwn",
        #                         "text": f"*Pipeline Event*\n*Pipeline:* {pipeline_name}\n*Event:* {event}"
        #                     }
        #                 }
        #             ]
        #         }
        #         if details:
        #             payload["blocks"].append({
        #                 "type": "section",
        #                 "text": {
        #                     "type": "mrkdwn",
        #                     "text": f"*Details:*\n```{json.dumps(details, indent=2)}```"
        #                 }
        #             })
        #         requests.post(webhook_url, json=payload)
        #     except Exception as e:
        #         logging.error(f"Failed to send Slack notification: {str(e)}")
    except Exception as e:
        logging.error(f"Failed to log pipeline execution: {str(e)}")


def run_pipeline(pipeline_name: str, dataset_name: str, table_name: str):
    start_time = time.time()
    result = execute_query("SELECT id, source_url FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True)
    if not result:
        logging.error(f"No source URL found for pipeline `{pipeline_name}`")
        send_slack_message(f"Pipeline `{pipeline_name}` failed: No source URL found.")
        return None

    pipeline_id, source_url = result[0]
    source_url = source_url.strip()
    source_url_lower = source_url.lower()
    logging.info(f"Normalized source URL: {source_url_lower}")

    # Get next pipeline run ID
    next_id_result = execute_query("SELECT COALESCE(MAX(id), 0) + 1 FROM pipeline_runs", fetch=True)
    run_id = next_id_result[0][0]

    # Create pipeline run entry
    execute_query(
        """
        INSERT INTO pipeline_runs (
            id, pipeline_id, pipeline_name, start_time, status,
            extract_status, normalize_status, load_status
        ) VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'running', 'pending', 'pending', 'pending')
        """,
        (run_id, pipeline_id, pipeline_name)
    )

    # If the source URL is an API endpoint (http), load API configuration
    if source_url_lower.startswith("http"):
        logging.info('Loading API configuration...')
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
        logging.info('Loading S3 configuration...')
        data = fetch_data_from_s3(pipeline_name)
        data_to_run = data
    elif source_url_lower.startswith(
        ("postgres", "mysql", "bigquery", "redshift", "mssql", "microsoft_sqlserver", "oracle")
    ):
        logging.info('Loading database configuration...')
        data_to_run = fetch_data_from_database(pipeline_name)
    else:
        logging.error(f"Unsupported source type for URL: {source_url}")
        return None

    if not data_to_run:
        # Update run status to failed
        execute_query(
            """
            UPDATE pipeline_runs 
            SET status = 'failed', 
                error_message = 'No data fetched',
                end_time = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (run_id,)
        )
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url, "error", "No data fetched")
        return None

    # Determine write disposition based on config incremental_type:
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
        log_pipeline_execution(pipeline_name, table_name, dataset_name, source_url,
                             "started", "Pipeline execution started", start_time=start_time)
        send_slack_message(f"Pipeline `{pipeline_name}` started at {start_time.isoformat()}.")

        # Update extract progress
        logging.info('Starting data extraction...')
        execute_query(
            "UPDATE pipeline_runs SET extract_status = 'running', extract_start_time = CURRENT_TIMESTAMP WHERE id = ?",
            (run_id,)
        )
        
        if write_disposition:
            pipeline.run(data_to_run, write_disposition=write_disposition)
        else:
            pipeline.run(data_to_run)

        # Update normalize progress
        logging.info('Normalizing data...')
        execute_query(
            "UPDATE pipeline_runs SET extract_status = 'completed', extract_end_time = CURRENT_TIMESTAMP, normalize_status = 'running', normalize_start_time = CURRENT_TIMESTAMP WHERE id = ?",
            (run_id,)
        )
        
        pipeline.run([pipeline.last_trace], table_name="_trace")

        # Update load progress
        logging.info('Loading to Snowflake...')
        execute_query(
            "UPDATE pipeline_runs SET normalize_status = 'completed', normalize_end_time = CURRENT_TIMESTAMP, load_status = 'running', load_start_time = CURRENT_TIMESTAMP WHERE id = ?",
            (run_id,)
        )

        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(), 2)
        trace = pipeline.last_trace
        row_counts = trace.last_normalize_info.row_counts if trace and trace.last_normalize_info else {}
        total_rows = sum(count for table, count in row_counts.items() if not table.startswith("_dlt_"))
        
        # Calculate rows per second
        rows_per_second = round(total_rows / duration, 2) if duration > 0 else 0
        
        logging.info(f'Pipeline completed! Processed {total_rows} rows at {rows_per_second} rows/sec')
        
        # Update pipeline run status
        execute_query(
            """
            UPDATE pipeline_runs 
            SET status = 'completed',
                end_time = CURRENT_TIMESTAMP,
                duration = ?,
                rows_processed = ?,
                load_status = 'completed',
                load_end_time = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (duration, total_rows, run_id)
        )
        
        # Update pipeline status
        execute_query(
            "UPDATE pipelines SET last_run_status = 'completed' WHERE id = ?",
            (pipeline_id,)
        )
        
        logging.info(f"Pipeline `{pipeline_name}` completed in {duration} seconds! Rows Loaded: {total_rows}")
        logging.info("Extract Info: %s", pipeline.last_trace.last_extract_info)
        logging.info("Normalize Info: %s", pipeline.last_trace.last_normalize_info)
        logging.info("Load Info: %s", pipeline.last_trace.last_load_info)
        logging.info("Row Counts: %s", pipeline.last_trace.last_normalize_info.row_counts)
        
        # Here we also try to log per-resource trace details if available.
        per_resource_details = ""
        if trace and hasattr(trace, "resource_traces"):
            for res_name, res_trace in trace.resource_traces.items():
                per_resource_details += f"\nResource '{res_name}': {res_trace}"
        else:
            per_resource_details = "No per-resource trace details available."

        log_pipeline_execution(
            pipeline_name, table_name, dataset_name, source_url,
            "completed", f"Completed in {duration} seconds. Rows Loaded: {total_rows}\nResource Details: {per_resource_details}",
            start_time=start_time, end_time=end_time, trace=trace
        )
        send_slack_message(f"Pipeline `{pipeline_name}` completed in {duration} seconds. Rows Loaded: {total_rows}.")
        return total_rows
    except Exception as e:
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(), 2)
        error_msg = f"Pipeline execution failed in {duration} seconds: {str(e)}"
        logging.error(error_msg)
        
        # Update pipeline run status to failed
        execute_query(
            """
            UPDATE pipeline_runs 
            SET status = 'failed',
                end_time = CURRENT_TIMESTAMP,
                duration = ?,
                error_message = ?,
                load_status = 'failed',
                load_end_time = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (duration, str(e), run_id)
        )
        
        # Update pipeline status
        execute_query(
            "UPDATE pipelines SET last_run_status = 'failed' WHERE id = ?",
            (pipeline_id,)
        )
        
        trace_obj = pipeline.last_trace if hasattr(pipeline, "last_trace") else None
        log_pipeline_execution(
            pipeline_name, table_name, dataset_name, source_url,
            "error", f"Failed in {duration} seconds: {str(e)}",
            start_time=start_time, end_time=end_time, trace=trace_obj
        )
        send_slack_message(f"Pipeline `{pipeline_name}` failed after {duration} seconds: {str(e)}")
        return None

def run_pipeline_with_creds(pipeline_name: str, dataset_name: str, table_name: str, creds: dict):
    """Runs a pipeline with the provided Snowflake credentials."""
    try:
        # Set environment variables for the pipeline
        set_env_vars(creds, pipeline_name)
        
        # Run the pipeline
        result = run_pipeline(pipeline_name, dataset_name, table_name)
        
        return result
    except Exception as e:
        logging.error(f"Error running pipeline with credentials: {str(e)}")
        return None