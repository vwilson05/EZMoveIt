import os
import logging
import json
import dlt
import time
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from src.sources.api_source import fetch_data_from_api
from src.sources.database_source import fetch_data_from_database
from src.sources.storage_source import fetch_data_from_s3
from src.db.duckdb_connection import execute_query

# --- **Helper Functions** ---
def get_config_path(filename):
    """Returns the correct config file path based on environment (Docker vs. local)."""
    running_in_docker = (
        os.getenv("RUNNING_IN_DOCKER", "").strip().lower()
    )  # Normalize value
    local_path = os.path.abspath(os.path.join(os.getcwd(), "config", filename))
    docker_path = f"/app/config/{filename}"

    logging.info(f"🔍 RUNNING_IN_DOCKER env var: {running_in_docker}")
    logging.info(f"🔍 Checking if running in Docker: {running_in_docker == 'true'}")

    # Ensure correct behavior
    if running_in_docker == "true":  # ✅ Explicitly check for "true"
        logging.info(f"📂 Using Docker path: {docker_path}")
        return docker_path
    else:
        logging.info(f"📂 Using local path: {local_path}")
        return local_path


def load_snowflake_credentials():
    """Loads Snowflake credentials and ensures the private key path is correctly resolved."""
    config_path = get_config_path(
        "snowflake_config.json"
    )  # ✅ Use centralized function

    if not os.path.exists(config_path):
        logging.error(f"❌ Snowflake config file missing! Expected at: {config_path}")
        return {}

    with open(config_path, "r") as f:
        creds = json.load(f)

    logging.info(
        f"🔍 Loaded Snowflake credentials: { {k: v if k != 'private_key' else 'HIDDEN' for k, v in creds.items()} }"
    )

    # ✅ Fix private_key_path dynamically
    if creds.get("authenticator") == "snowflake_jwt":
        original_key_path = creds.get(
            "private_key_path", "/app/config/rsa_key.p8"
        )  # Default Docker path
        corrected_key_path = get_config_path(
            os.path.basename(original_key_path)
        )  # ✅ Fix for local vs Docker

        creds["private_key_path"] = corrected_key_path  # ✅ Override JSON value

        logging.info(f"🔑 Updated Private key path: {corrected_key_path}")

        # ✅ Check if private key actually exists
        if not os.path.exists(corrected_key_path):
            logging.error(f"❌ Private key file missing at {corrected_key_path}!")
            return {}

        try:
            with open(corrected_key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                )
                creds["private_key"] = private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                ).decode("utf-8")  # ✅ Convert bytes to string for Snowflake

                logging.info(
                    "✅ Successfully loaded and embedded private key into credentials."
                )
        except Exception as e:
            logging.error(f"❌ Failed to load private key: {e}")
            return {}

    return creds


def load_private_key():
    """Loads the RSA private key in bytes for Snowflake authentication."""
    key_path = get_config_path("rsa_key.p8")
    logging.info(f"🔍 Checking for private key at: {key_path}")

    if not os.path.exists(key_path):
        logging.error(f"❌ Private key file missing! Expected at: {key_path}")
        return None

    try:
        with open(key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # If encrypted, set your passphrase here
                backend=default_backend(),
            )

        # ✅ Export as bytes in the correct format
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        logging.info("✅ Successfully loaded private key in correct format.")
        return private_key_bytes  # ✅ Return the key in bytes (needed for Snowflake)

    except Exception as e:
        logging.error(f"❌ Failed to load private key: {e}")
        return None


def set_env_vars(creds, pipeline_name):
    """Sets Snowflake credentials as environment variables dynamically."""
    if not creds:
        logging.error("❌ Missing or invalid Snowflake credentials!")
        return

    env_prefix = pipeline_name.upper()
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__ACCOUNT"] = (
        creds.get("account", "")
    )
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"] = (
        creds.get("username", "")
    )
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"] = creds.get(
        "role", ""
    )
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"] = (
        creds.get("database", "")
    )
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__SCHEMA"] = (
        creds.get("schema", "")
    )
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"] = creds.get(
        "host", ""
    )
    os.environ[f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__AUTHENTICATOR"] = (
        creds.get("authenticator", "")
    )
    os.environ[
        f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__SESSION_KEEP_ALIVE"
    ] = str(creds.get("session_keep_alive", "true")).lower()

    # ✅ Inject the actual private key
    if creds.get("private_key"):
        os.environ[
            f"{env_prefix}__DESTINATION__SNOWFLAKE__CREDENTIALS__PRIVATE_KEY"
        ] = creds["private_key"]
        logging.info("🔑 Private key successfully set in environment variables.")
    else:
        logging.warning("⚠️ Private key not found in credentials.")


def log_pipeline_execution(
    pipeline_name, table_name, dataset_name, source_url, event, message, duration=None
):
    """Logs pipeline execution details into `pipeline_logs` in DuckDB, ensuring pipeline_id is set."""

    # ✅ Retrieve the pipeline_id from the `pipelines` table
    result = execute_query(
        "SELECT id FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True
    )

    if not result:
        logging.error(
            f"❌ No matching pipeline ID found for `{pipeline_name}`. Log entry skipped."
        )
        return  # 🚨 Exit if no matching pipeline found

    pipeline_id = result[0][0]  # ✅ Extract pipeline ID

    # ✅ Fetch the next available ID manually for pipeline_logs
    log_result = execute_query("SELECT MAX(id) FROM pipeline_logs", fetch=True)
    next_id = (
        (log_result[0][0] + 1) if log_result and log_result[0][0] else 1
    )  # Start at 1 if empty

    # ✅ Insert log entry with correct `pipeline_id`
    query = """
    INSERT INTO pipeline_logs (id, pipeline_id, pipeline_name, source_url, snowflake_target, dataset_name, event, log_message, duration)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    execute_query(
        query,
        (
            next_id,
            pipeline_id,
            pipeline_name,
            source_url,
            table_name,
            dataset_name,
            event,
            message,
            duration,
        ),
    )

    logging.info(
        f"📝 Logged event `{event}` for pipeline `{pipeline_name}` (ID: {pipeline_id})"
    )


def run_pipeline(pipeline_name: str, dataset_name: str, table_name: str):
    """Runs the DLT pipeline dynamically for the specified source and logs execution."""

    # ✅ Fetch pipeline source details
    result = execute_query(
        "SELECT source_url FROM pipelines WHERE name = ?", (pipeline_name,), fetch=True
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
        log_pipeline_execution(
            pipeline_name,
            table_name,
            dataset_name,
            source_url,
            "error",
            "No data fetched",
        )
        return None

    # ✅ Load Snowflake credentials
    creds = load_snowflake_credentials()
    if not creds:
        logging.error("❌ Missing Snowflake credentials! Aborting pipeline execution.")
        log_pipeline_execution(
            pipeline_name,
            table_name,
            dataset_name,
            source_url,
            "error",
            "Missing Snowflake credentials",
        )
        return None

    set_env_vars(creds, pipeline_name)  # ✅ Apply Snowflake environment variables

    # ✅ Create DLT pipeline
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, destination="snowflake", dataset_name=dataset_name
    )

    try:
        start_time = time.time()  # ✅ Capture start time
        log_pipeline_execution(
            pipeline_name,
            table_name,
            dataset_name,
            source_url,
            "started",
            "Pipeline execution started",
        )

        # ✅ If API data, run with the named resource
        if source_url.startswith("http"):
            pipeline.run(api_data_resource)
        else:
            pipeline.run(data)

        # ✅ Capture duration and compute execution time
        duration = round(time.time() - start_time, 2)

        # ✅ Check if data was actually loaded
        row_counts = (
            pipeline.last_trace.last_normalize_info.row_counts
            if pipeline.last_trace and pipeline.last_trace.last_normalize_info
            else {}
        )
        total_rows = sum(
            count
            for table, count in row_counts.items()
            if not table.startswith("_dlt_")
        )

        logging.info(
            f"✅ Pipeline `{pipeline_name}` completed in {duration} seconds! Rows Loaded: {total_rows}"
        )
        log_pipeline_execution(
            pipeline_name,
            table_name,
            dataset_name,
            source_url,
            "completed",
            f"Completed in {duration} seconds. Rows Loaded: {total_rows}",
            duration,
        )
        return total_rows

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        logging.error(f"❌ Pipeline execution failed in {duration} seconds: {str(e)}")
        log_pipeline_execution(
            pipeline_name,
            table_name,
            dataset_name,
            source_url,
            "error",
            f"Failed in {duration} seconds: {str(e)}",
            duration,
        )
        return None
