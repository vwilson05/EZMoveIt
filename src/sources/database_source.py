import logging
import json
import os
from sqlalchemy import create_engine
import dlt
import pendulum
from dlt.sources.sql_database import sql_table, sql_database
from itertools import islice

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")

def paginate_generator(gen, chunk_size=50000):
    """Yield chunks (lists) of rows from the generator."""
    while True:
        chunk = list(islice(gen, chunk_size))
        if not chunk:
            break
        yield chunk

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
    incremental_type = db_config.get("incremental_type", "FULL").upper()
    primary_key = db_config.get("primary_key")
    delta_column = db_config.get("delta_column")
    delta_value = db_config.get("delta_value")

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
            res = sql_table(
                engine,
                table=table_name,
                schema=schema_name,
                incremental=dlt.sources.incremental(delta_column, initial_value=initial_dt)
            ).apply_hints(primary_key=primary_key).parallelize()
        else:
            res = sql_table(engine, table=table_name, schema=schema_name).parallelize()
        # For single table mode, simply wrap the generator.
        return paginate_generator(res, 50000)

    elif mode == "sql_database":
        if not schema_name:
            logging.error("❌ Schema is required in sql_database mode!")
            return None
        logging.info(f"Loading schema '{schema_name}' from database.")
        source = sql_database(engine, schema=schema_name).parallelize()
        table_list = db_config.get("tables")
        if table_list and len(table_list) > 0:
            logging.info(f"Selecting table subset: {table_list}")
            source = source.with_resources(*table_list)
        else:
            logging.info("No specific table subset provided; loading all tables in schema.")

        if incremental_type == "INCREMENTAL" and delta_column and delta_value and primary_key:
            initial_dt = pendulum.parse(delta_value)
            for tbl in source.resources:
                logging.info(f"Applying incremental hint to table '{tbl}' on column '{delta_column}'")
                source.resources[tbl] = source.resources[tbl].apply_hints(
                    primary_key=primary_key,
                    incremental=dlt.sources.incremental(delta_column, initial_value=initial_dt)
                )
        # Instead of wrapping with a new function object, modify each resource's __call__ method in place.
        for tbl, resource in source.resources.items():
            original_call = resource.__call__
            def new_call(*args, _orig=original_call, **kwargs):
                return paginate_generator(_orig(*args, **kwargs), 50000)
            resource.__call__ = new_call
        return source

    else:
        logging.error("❌ Unknown mode specified in db_config")
        return None

if __name__ == "__main__":
    test_pipeline_name = "test_pipeline"
    resource = fetch_data_from_database(test_pipeline_name)
    if resource:
        logging.info("Successfully fetched database source.")
    else:
        logging.error("Failed to fetch database source.")
