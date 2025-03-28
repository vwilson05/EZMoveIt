import logging
import json
import os
from sqlalchemy import create_engine
import dlt
import pendulum
from dlt.sources.sql_database import sql_table, sql_database

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")

def load_db_config(pipeline_name):
    config_path = os.path.join(
        CONFIG_DIR, f"{pipeline_name.replace(' ', '_').lower()}_config.json"
    )
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}

def fetch_data_from_database(pipeline_name):
    """
    Returns a DLT source/resource for SQL database access.

    Modes:
      - "sql_table": loads a single table.
      - "sql_database": loads an entire schema or a selected subset.

    For incremental loads:
      - We pass an incremental hint to the resource via dlt.sources.incremental.
      - The initial_value provided (converted from the delta_value string) is only used
        on the very first run when no state exists.
      - DLT stores the incremental state (e.g., in a system table like _dlt_state in Snowflake)
        and uses it in subsequent runs.
      - The primary key hint is then applied to support merge (upsert) operations.
    """
    db_config = load_db_config(pipeline_name)
    if not db_config:
        logging.error(f"❌ No database config found for `{pipeline_name}`!")
        return None

    # Build connection string.
    conn_str = db_config.get("credentials")
    if not conn_str:
        db_type = db_config.get("db_type", "").lower()
        user = db_config.get("user", "loader")
        password = db_config.get("password", "loader")
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 1433 if db_type in ["mssql", "microsoft_sqlserver"] else 1521)
        database = db_config.get("database", "dlt_data")
        if db_type in ["mssql", "microsoft_sqlserver"]:
            driver = db_config.get("driver", "ODBC+Driver+17+for+SQL+Server")
            conn_str = (
                f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}"
                f"?TrustServerCertificate=yes&driver={driver}"
            )
            logging.info(f"Built SQL Server connection string: {conn_str}")
        elif db_type == "oracle":
            service_name = db_config.get("service_name", "orcl")
            conn_str = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
            logging.info(f"Built Oracle connection string: {conn_str}")
        else:
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            logging.info(f"Built PostgreSQL connection string: {conn_str}")
    else:
        logging.info(f"Using provided connection string: {conn_str}")

    engine = create_engine(conn_str)
    logging.info("SQLAlchemy engine created.")

    # Incremental settings.
    incremental_type = db_config.get("incremental_type", "FULL").upper()  # "FULL" or "INCREMENTAL"
    primary_key = db_config.get("primary_key")      # e.g., "CustomerID"
    delta_column = db_config.get("delta_column")      # e.g., "updated_dt"
    delta_value = db_config.get("delta_value")        # e.g., "1900-01-01"
    # Note: DLT will store its incremental state (e.g., in _dlt_state) after a successful run.
    # The initial_value here is used only if no state exists yet.

    mode = db_config.get("mode", "sql_table")
    schema_name = db_config.get("schema")

    if mode == "sql_table":
        table_name = db_config.get("table")
        if not table_name:
            logging.error("❌ No table name specified in config for single table mode!")
            return None

        logging.info(f"Configuring sql_table for {schema_name}.{table_name}" if schema_name else f"Configuring sql_table for {table_name}")
        if incremental_type == "INCREMENTAL" and delta_column and delta_value and primary_key:
            initial_dt = pendulum.parse(delta_value)
            # The incremental hint is passed to sql_table.
            res = sql_table(
                engine,
                table=table_name,
                schema=schema_name,
                incremental=dlt.sources.incremental(delta_column, initial_value=initial_dt).parallelize()
            )
            res = res.apply_hints(primary_key=primary_key)
        else:
            res = sql_table(engine, table=table_name, schema=schema_name)
        # In single table mode, the resource from sql_table() should already have a name.
        return res

    elif mode == "sql_database":
        if not schema_name:
            logging.error("❌ Schema is required in sql_database mode!")
            return None
        logging.info(f"Loading schema '{schema_name}' from database.")
        # Create a full-schema source.
        source = sql_database(engine, schema=schema_name).parallelize()
        table_list = db_config.get("tables")  # List of tables to load.
        if table_list and len(table_list) > 0:
            logging.info(f"Selecting table subset: {table_list}")
            # with_resources restricts the source to the specified tables.
            source = source.with_resources(*table_list)
        else:
            logging.info("No specific table subset provided; loading all tables in schema.")

        # Apply incremental hints to each resource if configured.
        if incremental_type == "INCREMENTAL" and delta_column and delta_value and primary_key:
            initial_dt = pendulum.parse(delta_value)
            for tbl in source.resources:
                logging.info(f"Applying incremental hint to table '{tbl}' on column '{delta_column}'")
                source.resources[tbl] = source.resources[tbl].apply_hints(
                    primary_key=primary_key,
                    incremental=dlt.sources.incremental(delta_column, initial_value=initial_dt)
                )
        # The source returned by sql_database() has resources keyed by table name.
        return source

    else:
        logging.error("❌ Unknown mode specified in db_config")
        return None

if __name__ == "__main__":
    # For testing purposes:
    test_pipeline_name = "test_pipeline"
    resource = fetch_data_from_database(test_pipeline_name)
    if resource:
        logging.info("Successfully fetched database source.")
    else:
        logging.error("Failed to fetch database source.")
