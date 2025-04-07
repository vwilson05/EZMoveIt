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
# from config.slack_config import load_slack_config

# load_slack_config()

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


def run_pipeline(pipeline_name: str, dataset_name: str, table_name: str, run_id: int = None):
    start_time = time.time()
    result = execute_query(
        "SELECT id, source_url, metadata_selection FROM pipelines WHERE name = ?",
        (pipeline_name,),
        fetch=True
    )
    if not result:
        logging.error(f"No source URL found for pipeline `{pipeline_name}`")
        send_slack_message(f"Pipeline `{pipeline_name}` failed: No source URL found.")
        return None

    pipeline_id, source_url, metadata_selection_json = result[0]
    source_url = source_url.strip()
    source_url_lower = source_url.lower()
    logging.info(f"Normalized source URL: {source_url_lower}")

    # Parse metadata_selection if it exists
    metadata_selection = json.loads(metadata_selection_json) if metadata_selection_json else None
    logging.info(f"Metadata selection: {metadata_selection}")

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

    # If this is a metadata-driven pipeline, update the source_config dynamically
    if metadata_selection:
        logging.info(f"Metadata-driven pipeline detected. Selection criteria: {metadata_selection}")
        
        # Determine how to fetch metadata based on selection type
        if metadata_selection["type"] == "explicit":
            # Explicit object selection - fetch by IDs
            object_ids = metadata_selection["object_ids"]
            load_type = metadata_selection["load_type"]
            
            # Fetch current metadata for the selected objects
            id_placeholders = ", ".join(["?" for _ in object_ids])
            query = f"""
            SELECT 
                id, source_type, driver_type, logical_name, hostname, port,
                database_name, schema_name, table_name, source_url, endpoint,
                load_type, primary_key, delta_column, delta_value
            FROM metadata_config
            WHERE id IN ({id_placeholders})
            """
            params = object_ids
            
        else:  # Filter-based selection
            # Build a query based on filter criteria
            load_type = metadata_selection["load_type"]
            filters = metadata_selection["filters"]
            
            query_parts = ["SELECT id, source_type, driver_type, logical_name, hostname, port, "
                          "database_name, schema_name, table_name, source_url, endpoint, "
                          "load_type, primary_key, delta_column, delta_value "
                          "FROM metadata_config WHERE 1=1"]
            params = []
            
            # Add filter conditions
            if filters.get("source_type"):
                query_parts.append("AND source_type = ?")
                params.append(filters["source_type"])
            
            if filters.get("logical_name"):
                query_parts.append("AND logical_name = ?")
                params.append(filters["logical_name"])
                
            if filters.get("database_name"):
                query_parts.append("AND database_name = ?")
                params.append(filters["database_name"])
                
            if filters.get("schema_name"):
                query_parts.append("AND schema_name = ?")
                params.append(filters["schema_name"])
            
            # Always filter by the pipeline's load type
            query_parts.append("AND load_type = ?")
            params.append(load_type)
            
            # Construct the final query
            query = " ".join(query_parts)
            logging.info(f"Filter-based query: {query} with params: {params}")
        
        # Execute the query to get current metadata
        current_metadata = execute_query(query, params=params, fetch=True)
        
        if current_metadata:
            logging.info(f"Found {len(current_metadata)} objects matching selection criteria")
            
            # Group by source connection to handle multiple sources
            source_groups = {}
            for record in current_metadata:
                # Create key based on source type
                if record[1] in ["SQL Server", "Oracle"]:  # Database sources
                    source_key = f"{record[1]}_{record[4]}_{record[6]}"  # source_type_hostname_database
                else:  # API sources
                    source_key = f"{record[1]}_{record[9]}"  # source_type_url
                    
                if source_key not in source_groups:
                    source_groups[source_key] = {
                        "source_type": record[1],
                        "driver_type": record[2],
                        "hostname": record[4],
                        "port": record[5],
                        "database_name": record[6],
                        "schema_name": record[7],
                        "source_url": record[9],
                        "tables": []
                    }
                
                # Add table info
                source_groups[source_key]["tables"].append({
                    "table_name": record[8],
                    "primary_key": record[12],
                    "delta_column": record[13],
                    "delta_value": record[14]
                })
            
            # Log all found source groups
            logging.info(f"Source groups: {json.dumps(source_groups)}")
            
            # For now, we'll use the first source group (most pipelines will have just one)
            # In a future enhancement, we could handle multiple source groups
            if source_groups:
                primary_source = list(source_groups.values())[0]
                
                # Update the source_config based on source type
                if primary_source["source_type"] in ["SQL Server", "Oracle"]:
                    # Database configuration
                    db_type = primary_source["source_type"].lower().replace(" ", "_")
                    source_config.update({
                        "db_type": db_type,
                        "mode": "sql_database" if len(primary_source["tables"]) > 1 else "sql_table",
                        "host": primary_source["hostname"],
                        "port": primary_source["port"],
                        "database": primary_source["database_name"],
                        "schema": primary_source["schema_name"],
                        "incremental_type": load_type.upper(),
                        "use_parallel": True,
                        "chunk_size": source_config.get("chunk_size", 100000)
                    })
                    
                    if source_config["mode"] == "sql_table":
                        # Single table mode
                        source_config["table"] = primary_source["tables"][0]["table_name"]
                        if load_type.lower() == "incremental":
                            source_config["primary_key"] = primary_source["tables"][0]["primary_key"]
                            source_config["delta_column"] = primary_source["tables"][0]["delta_column"]
                            source_config["delta_value"] = primary_source["tables"][0]["delta_value"]
                    else:
                        # Multiple tables mode - IMPORTANT: This is the key change
                        source_config["tables"] = [t["table_name"] for t in primary_source["tables"]]
                        
                        # For incremental loads in multi-table mode
                        if load_type.lower() == "incremental" and primary_source["tables"]:
                            source_config["primary_key"] = primary_source["tables"][0]["primary_key"]
                            source_config["delta_column"] = primary_source["tables"][0]["delta_column"]
                            source_config["delta_value"] = primary_source["tables"][0]["delta_value"]
                    
                    # For SQL Server, add driver if needed
                    if db_type == "sql_server":
                        source_config["driver"] = primary_source["driver_type"] or "ODBC+Driver+17+for+SQL+Server"
                        
                    logging.info(f"Updated source config from metadata: {source_config}")
                    
                    # Update the source URL as well if it was determined from metadata
                    if db_type == "sql_server":
                        source_url = f"microsoft_sqlserver://{primary_source['hostname']}:{primary_source['port']}/{primary_source['database_name']}"
                    elif db_type == "oracle":
                        source_url = f"oracle://{primary_source['hostname']}:{primary_source['port']}/{primary_source['database_name']}"
                    
                    source_url_lower = source_url.lower()
                    logging.info(f"Updated source URL from metadata: {source_url}")

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
        db_config = load_db_config(pipeline_name)
        # Log performance settings
        logging.info(f"Performance settings: parallel={db_config.get('use_parallel', True)}, "
                    f"chunk_size={db_config.get('chunk_size', 100000)}")
        data_to_run = fetch_data_from_database(pipeline_name, run_id)
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