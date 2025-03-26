import logging
import json
import os
from sqlalchemy import create_engine
from dlt.sources.sql_database import sql_database, sql_table

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
    Returns a dlt resource configured for a SQL database source using a SQLAlchemy engine.
    
    In single-table mode, it uses:
        sql_table(engine, table=table_name, schema=schema)
    In full-database mode, it uses:
        sql_database(engine, schema=schema) if schema is provided,
        otherwise sql_database(engine).
    
    The connection string is built from config values unless a full
    "credentials" string is provided.
    """
    db_config = load_db_config(pipeline_name)
    if not db_config:
        logging.error(f"❌ No database config found for `{pipeline_name}`!")
        return None

    # Build connection string from config if not provided.
    conn_str = db_config.get("credentials")
    if not conn_str:
        db_type = db_config.get("db_type", "").lower()
        user = db_config.get("user", "loader")
        password = db_config.get("password", "loader")
        host = db_config.get("host", "localhost")
        # Use 1433 for SQL Server, 5432 for others.
        port = db_config.get("port", 1433 if db_type in ["mssql", "microsoft_sqlserver"] else 5432)
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
            conn_str = f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}"
            logging.info(f"Built Oracle connection string: {conn_str}")
        else:
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            logging.info(f"Built PostgreSQL connection string: {conn_str}")
    else:
        logging.info(f"Using provided connection string: {conn_str}")

    # Create the SQLAlchemy engine.
    engine = create_engine(conn_str)
    logging.info("SQLAlchemy engine created.")

    mode = db_config.get("mode", "sql_table")
    if mode == "sql_table":
        table_name = db_config.get("table")
        if not table_name:
            logging.error("❌ No table name specified in config for single table mode!")
            return None
        schema = db_config.get("schema")  # e.g., "retail_demo"
        if schema:
            logging.info(f"Configuring SQL table resource for {schema}.{table_name}")
            resource = sql_table(engine, table=table_name, schema=schema)
        else:
            logging.info(f"Configuring SQL table resource for {table_name}")
            resource = sql_table(engine, table=table_name)
        logging.info(f"Configured SQL table resource using engine: {conn_str}")
        return resource

    elif mode == "sql_database":
        # When loading the entire DB, optionally restrict to a specific schema.
        schema = db_config.get("schema")
        if schema:
            logging.info(f"Configuring SQL database resource for schema '{schema}'")
            resource = sql_database(engine, schema=schema)
        else:
            resource = sql_database(engine)
        logging.info(f"Configured SQL database resource (all tables) using engine: {conn_str}")
        return resource

    else:
        logging.error("❌ Unknown mode specified in db_config")
        return None
