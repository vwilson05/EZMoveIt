import streamlit as st
import threading
import logging
import json
import os
import time
from datetime import datetime
import pandas as pd
from src.pipelines.dlt_pipeline import run_pipeline_with_creds
from src.db.duckdb_connection import execute_query
import dlt
from sqlalchemy import text

CONFIG_DIR = "config"
os.makedirs(CONFIG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_sources_by_category():
    return {
        "API": [
            "REST API - Public",
            "REST API - Private"
        ],
        "Database": [
            "SQL Server",
            "Oracle",
            "Snowflake",
            "PostgreSQL",
            "MySQL"
        ]
    }


def get_internal_source_type(display_name):
    """Maps display names to DLT source types"""
    mapping = {
        "REST API - Public": "rest_api",
        "REST API - Private": "rest_api",
        "SQL Server": "microsoft_sqlserver",
        "Oracle": "oracle",
        "Snowflake": "snowflake",
        "PostgreSQL": "postgresql",
        "MySQL": "mysql"
    }
    return mapping.get(display_name)


def get_next_pipeline_id():
    result = execute_query("SELECT MAX(id) FROM pipelines", fetch=True)
    next_id = (result[0][0] + 1) if result and result[0][0] is not None else 1
    return next_id


def get_next_pipeline_run_id():
    result = execute_query("SELECT MAX(id) FROM pipeline_runs", fetch=True)
    next_id = (result[0][0] + 1) if result and result[0][0] is not None else 1
    return next_id


def save_source_config(source_key, config_data):
    config_path = os.path.join(CONFIG_DIR, f"{source_key.replace(' ', '_').lower()}_config.json")
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=2, default=str)


@dlt.source
def selected_tables_source(engine, schema: str, table_names: list[str]):
    logging.info(f"Creating source from tables: {table_names} in schema: {schema}")
    resources = {tbl: dlt.sources.sql_database.sql_table(engine, table=tbl, schema=schema) for tbl in table_names}
    return resources


def get_source_configs():
    """Get available source configurations."""
    query = """
    SELECT 
        name,
        source_type,
        source_url,
        config
    FROM source_configs
    ORDER BY name
    """
    return execute_query(query, fetch=True)


def pipeline_creator_page():
    st.title("➕ Create Pipeline")
    st.caption("Create a new data pipeline to extract, transform, and load data.")
    
    # Initialize session state for form data if not exists
    if 'pipeline_name' not in st.session_state:
        st.session_state.pipeline_name = ""
    if 'config_mode' not in st.session_state:
        st.session_state.config_mode = "manual"
    if 'selected_source' not in st.session_state:
        st.session_state.selected_source = None
    if 'source_config' not in st.session_state:
        st.session_state.source_config = {}
    if 'source_url' not in st.session_state:
        st.session_state.source_url = ""
    if 'target_table' not in st.session_state:
        st.session_state.target_table = ""
    if 'dataset_name' not in st.session_state:
        st.session_state.dataset_name = ""
    if 'schedule_option' not in st.session_state:
        st.session_state.schedule_option = "manual"
    if 'start_time_val' not in st.session_state:
        st.session_state.start_time_val = datetime.now()
    if 'interval_minutes' not in st.session_state:
        st.session_state.interval_minutes = 60
    if 'use_metadata_config' not in st.session_state:
        st.session_state.use_metadata_config = False
    
    # Pipeline Name
    st.session_state.pipeline_name = st.text_input(
        "Pipeline Name",
        value=st.session_state.pipeline_name,
        placeholder="e.g., API-private-spotify-playlist"
    )
    
    # Configuration Mode
    st.session_state.config_mode = st.radio(
        "Configuration Mode",
        ["Manual", "JSON"],
        horizontal=True
    )
    
    # Metadata Configuration Toggle
    st.session_state.use_metadata_config = st.checkbox(
        "Use Metadata Configuration",
        value=st.session_state.use_metadata_config,
        help="Use predefined metadata configuration for the pipeline"
    )
    
    if st.session_state.use_metadata_config:
        # Get available metadata configurations
        metadata_configs = get_source_configs()
        if metadata_configs:
            config_names = [config[0] for config in metadata_configs]
            selected_config = st.selectbox(
                "Select Metadata Configuration",
                options=config_names,
                help="Choose a predefined metadata configuration"
            )
            
            # Load selected configuration
            if selected_config:
                selected_config_data = next(
                    (config for config in metadata_configs if config[0] == selected_config),
                    None
                )
                if selected_config_data:
                    st.session_state.source_config = json.loads(selected_config_data[3])
                    st.session_state.source_url = selected_config_data[2]
                    st.session_state.selected_source = selected_config_data[1]
        else:
            st.warning("No metadata configurations available. Please create one in the settings page.")
            st.session_state.use_metadata_config = False
    
    if not st.session_state.use_metadata_config:
        if st.session_state.config_mode == "Manual":
            # Source Selection
            source_configs = get_source_configs()
            
            # Use the source mapping to get categorized sources
            source_categories = get_sources_by_category()
            source_category = st.selectbox(
                "Source Category",
                list(source_categories.keys())
            )
            
            # Get available sources for the selected category
            available_sources = source_categories.get(source_category, [])
            st.session_state.selected_source = st.selectbox(
                "Select Source Type",
                available_sources
            )
            
            # Source Configuration
            if st.session_state.selected_source in ["REST API - Public", "REST API - Private"]:
                st.session_state.source_url = st.text_input(
                    "API Endpoint URL",
                    value=st.session_state.source_url,
                    placeholder="https://api.example.com/data"
                )
                
                # API Authentication
                auth_type = st.selectbox(
                    "Authentication Type",
                    ["API Key", "OAuth2", "Basic Auth", "Bearer Token"],
                    index=0
                )
                
                # Initialize source_config with empty dict
                st.session_state.source_config = {
                    "auth_type": "none",
                    "source_type": "rest_api"
                }
                
                if auth_type == "API Key":
                    api_key = st.text_input("API Key", type="password")
                    api_key_header = st.text_input("API Key Header Name", value="X-API-Key")
                    st.session_state.source_config = {
                        "auth_type": "api_key",
                        "api_key": api_key,
                        "api_key_header": api_key_header,
                        "source_type": "rest_api"
                    }
                elif auth_type == "OAuth2":
                    client_id = st.text_input("Client ID")
                    client_secret = st.text_input("Client Secret", type="password")
                    token_url = st.text_input("Token URL")
                    st.session_state.source_config = {
                        "auth_type": "oauth2",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "token_url": token_url,
                        "source_type": "rest_api"
                    }
                elif auth_type == "Basic Auth":
                    username = st.text_input("Username")
                    password = st.text_input("Password", type="password")
                    st.session_state.source_config = {
                        "auth_type": "basic",
                        "username": username,
                        "password": password,
                        "source_type": "rest_api"
                    }
                elif auth_type == "Bearer Token":
                    bearer_token = st.text_input("Bearer Token", type="password")
                    st.session_state.source_config = {
                        "auth_type": "bearer",
                        "auth": {
                            "Authorization": f"Bearer {bearer_token}"
                        },
                        "source_type": "rest_api"
                    }
                
                # Incremental Load Settings
                incremental_load = st.checkbox("Enable Incremental Load")
                if incremental_load:
                    incremental_field = st.text_input("Incremental Field Name", placeholder="e.g., updated_at")
                    st.session_state.source_config["incremental_load"] = {
                        "enabled": True,
                        "field": incremental_field
                    }
                
                # Add Advanced Settings expander
                with st.expander("Advanced Settings"):
                    st.write("API Performance Settings")
                    
                    # Pagination settings
                    pagination_type = st.selectbox(
                        "Pagination Type",
                        ["none", "page_number", "offset", "cursor"],
                        help="Select the pagination method used by the API"
                    )
                    
                    if pagination_type != "none":
                        col1, col2 = st.columns(2)
                        with col1:
                            page_size = st.number_input(
                                "Page Size",
                                min_value=1,
                                max_value=1000,
                                value=100,
                                help="Number of items per page"
                            )
                        with col2:
                            parallel_requests = st.number_input(
                                "Parallel Requests",
                                min_value=1,
                                max_value=10,
                                value=4,
                                help="Number of concurrent API requests"
                            )
                        
                        max_pages = st.number_input(
                            "Max Pages (optional)",
                            min_value=1,
                            value=None,
                            help="Maximum number of pages to fetch (leave empty for unlimited)"
                        )
                        
                        if pagination_type == "cursor":
                            cursor_path = st.text_input(
                                "Cursor Path",
                                placeholder="next_cursor",
                                help="JSON path to the cursor value in the response"
                            )
                    
                    # Update source config with pagination settings
                    st.session_state.source_config.update({
                        "pagination": {
                            "type": pagination_type,
                            "page_size": page_size if pagination_type != "none" else None,
                            "max_pages": max_pages if pagination_type != "none" else None,
                            "cursor_path": cursor_path if pagination_type == "cursor" else None
                        },
                        "parallel_requests": parallel_requests if pagination_type != "none" else 1
                    })
            
            elif st.session_state.selected_source in source_categories["Database"]:
                # Get the internal source type for storage
                source_type = get_internal_source_type(st.session_state.selected_source)
                
                # Initialize the db_config dictionary with default values
                db_config = {
                    "db_type": source_type,
                    "mode": "sql_table",  # default mode
                    "use_parallel": True,
                    "chunk_size": 100000,
                    "incremental_type": "FULL"
                }
                
                # Database connection form
                col1, col2 = st.columns(2)
                with col1:
                    db_config["host"] = st.text_input("Host", placeholder="localhost")
                    db_config["port"] = st.number_input(
                        "Port", 
                        min_value=1, 
                        max_value=65535, 
                        value=1433 if st.session_state.selected_source == "SQL Server" else (
                            1521 if st.session_state.selected_source == "Oracle" else (
                                443 if st.session_state.selected_source == "Snowflake" else 5432
                            )
                        )
                    )
                    db_config["database"] = st.text_input("Database Name")
                
                with col2:
                    db_config["user"] = st.text_input("Username")
                    db_config["password"] = st.text_input("Password", type="password")
                    
                    # Oracle-specific fields
                    if st.session_state.selected_source == "Oracle":
                        db_config["service_name"] = st.text_input("Service Name", value="orcl")
                    
                    # SQL Server-specific fields
                    if st.session_state.selected_source == "SQL Server":
                        db_config["driver"] = st.text_input("ODBC Driver", value="ODBC Driver 17 for SQL Server")
                        db_config["TrustServerCertificate"] = "yes"
                        db_config["LongAsMax"] = "yes"
                    
                    # Snowflake-specific fields
                    if st.session_state.selected_source == "Snowflake":
                        db_config["account"] = st.text_input("Account", placeholder="orgname-accountname")
                        db_config["warehouse"] = st.text_input("Warehouse", placeholder="compute_wh")
                        db_config["role"] = st.text_input("Role (optional)", placeholder="ACCOUNTADMIN")
                        db_config["schema"] = st.text_input("Schema", placeholder="PUBLIC")
                
                # Build connection string for display
                if st.session_state.selected_source == "SQL Server":
                    conn_str = f"mssql+pyodbc://{db_config['user']}:****@{db_config['host']}:{db_config['port']}/{db_config['database']}?driver={db_config['driver'].replace(' ', '+')}"
                    source_url = f"microsoft_sqlserver://{db_config['host']}:{db_config['port']}/{db_config['database']}"
                elif st.session_state.selected_source == "Oracle":
                    conn_str = f"oracle+oracledb://{db_config['user']}:****@{db_config['host']}:{db_config['port']}/?service_name={db_config['service_name']}"
                    source_url = f"oracle://{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
                elif st.session_state.selected_source == "Snowflake":
                    conn_str = f"snowflake://{db_config['user']}:****@{db_config['account']}/{db_config['database']}/{db_config['schema']}?warehouse={db_config['warehouse']}"
                    source_url = f"snowflake://{db_config['account']}/{db_config['database']}"
                
                st.session_state.source_url = source_url
                
                # Show the connection string (masked password)
                st.code(conn_str, language="text")
                
                # Table selection
                table_mode = st.radio(
                    "Table Selection Mode",
                    ["Single Table", "Multiple Tables"],
                    horizontal=True
                )
                
                if table_mode == "Single Table":
                    db_config["mode"] = "sql_table"
                    schema_name = st.text_input("Schema Name")
                    table_name = st.text_input("Table Name")
                    db_config["table"] = table_name
                    if schema_name:
                        db_config["schema"] = schema_name
                else:
                    db_config["mode"] = "sql_database"
                    schema_name = st.text_input("Schema Name", placeholder="public")
                    tables = st.text_input(
                        "Tables (comma-separated, leave empty for all)", 
                        placeholder="table1,table2,table3"
                    )
                    db_config["schema"] = schema_name
                    if tables:
                        db_config["tables"] = [t.strip() for t in tables.split(",")]
                
                # Store the configuration
                st.session_state.source_config = db_config
            
            elif st.session_state.selected_source == "File":
                file_type = st.selectbox("File Type", ["CSV", "JSON", "Excel"])
                file_path = st.text_input("File Path", placeholder="/path/to/file")
                
                st.session_state.source_config = {
                    "file_type": file_type,
                    "file_path": file_path
                }
        
        else:  # JSON mode
            json_config = st.text_area(
                "Paste JSON Configuration",
                value=json.dumps(st.session_state.source_config, indent=2),
                height=200
            )
            try:
                st.session_state.source_config = json.loads(json_config)
            except json.JSONDecodeError:
                st.error("Invalid JSON format")
    
    # Target Configuration
    st.markdown("---")
    st.subheader("Target Configuration")
    
    st.session_state.dataset_name = st.text_input(
        "Dataset Name",
        value=st.session_state.dataset_name,
        placeholder="e.g., API_SPOTIFY"
    )
    
    st.session_state.target_table = st.text_input(
        "Target Table",
        value=st.session_state.target_table,
        placeholder="e.g., PLAYLIST_1"
    )
    
    # Schedule Configuration
    st.markdown("---")
    st.subheader("Schedule Configuration")
    
    st.session_state.schedule_option = st.radio(
        "Schedule Type",
        ["Manual", "Interval", "Daily", "Weekly"],
        horizontal=True
    )
    
    if st.session_state.schedule_option == "Interval":
        st.session_state.interval_minutes = st.number_input(
            "Interval (minutes)",
            min_value=1,
            max_value=1440,
            value=st.session_state.interval_minutes
        )
    elif st.session_state.schedule_option == "Daily":
        # Convert datetime to time if needed
        if isinstance(st.session_state.start_time_val, datetime):
            st.session_state.start_time_val = st.session_state.start_time_val.time()
        st.session_state.start_time_val = st.time_input(
            "Start Time",
            value=st.session_state.start_time_val
        )
    elif st.session_state.schedule_option == "Weekly":
        # Convert datetime to time if needed
        if isinstance(st.session_state.start_time_val, datetime):
            st.session_state.start_time_val = st.session_state.start_time_val.time()
        st.session_state.start_time_val = st.time_input(
            "Start Time",
            value=st.session_state.start_time_val
        )
        weekday = st.selectbox(
            "Day of Week",
            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        )
    
    # Replace the existing submit button section with new buttons
    st.write("---")  # Add a visual separator
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Create Pipeline", type="primary", use_container_width=True):
            # Validate inputs
            if not st.session_state.pipeline_name:
                st.error("Please enter a pipeline name.")
                return
            if not st.session_state.source_url:
                st.error("Please enter a source URL.")
                return
            if not st.session_state.target_table:
                st.error("Please enter a target table name.")
                return
            
            # Save pipeline configuration
            pipeline_id = get_next_pipeline_id()
            save_source_config(st.session_state.pipeline_name, st.session_state.source_config)
            
            # Insert pipeline into database
            insert_query = """
            INSERT INTO pipelines (
                id, 
                name, 
                source_url, 
                target_table, 
                dataset_name, 
                schedule, 
                source_config,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            
            # Prepare schedule JSON if not manual
            schedule = None
            if st.session_state.schedule_option != "Manual":
                schedule = {
                    "type": st.session_state.schedule_option.lower(),
                    "interval_minutes": st.session_state.interval_minutes if st.session_state.schedule_option == "Interval" else None,
                    "start_time": st.session_state.start_time_val.strftime("%H:%M:%S") if st.session_state.schedule_option in ["Daily", "Weekly"] else None,
                    "weekday": weekday if st.session_state.schedule_option == "Weekly" else None
                }

            execute_query(
                insert_query,
                params=(
                    pipeline_id,
                    st.session_state.pipeline_name,
                    st.session_state.source_url,
                    st.session_state.target_table,
                    st.session_state.dataset_name,
                    json.dumps(schedule) if schedule else None,
                    json.dumps(st.session_state.source_config),
                )
            )
            
            # Navigate to Pipelines page
            st.success("Pipeline created successfully!")
            time.sleep(1)  # Brief pause for user feedback
            st.session_state.current_page = "Pipeline List"
            st.rerun()
    
    with col2:
        if st.button("Create + Run Pipeline", type="primary", use_container_width=True):
            # Validate inputs
            if not st.session_state.pipeline_name:
                st.error("Please enter a pipeline name.")
                return
            if not st.session_state.source_url:
                st.error("Please enter a source URL.")
                return
            if not st.session_state.target_table:
                st.error("Please enter a target table name.")
                return
            
            # Save pipeline configuration
            pipeline_id = get_next_pipeline_id()
            save_source_config(st.session_state.pipeline_name, st.session_state.source_config)
            
            # Insert pipeline into database
            insert_query = """
            INSERT INTO pipelines (
                id, 
                name, 
                source_url, 
                target_table, 
                dataset_name, 
                schedule, 
                source_config,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            
            # Prepare schedule JSON if not manual
            schedule = None
            if st.session_state.schedule_option != "Manual":
                schedule = {
                    "type": st.session_state.schedule_option.lower(),
                    "interval_minutes": st.session_state.interval_minutes if st.session_state.schedule_option == "Interval" else None,
                    "start_time": st.session_state.start_time_val.strftime("%H:%M:%S") if st.session_state.schedule_option in ["Daily", "Weekly"] else None,
                    "weekday": weekday if st.session_state.schedule_option == "Weekly" else None
                }

            execute_query(
                insert_query,
                params=(
                    pipeline_id,
                    st.session_state.pipeline_name,
                    st.session_state.source_url,
                    st.session_state.target_table,
                    st.session_state.dataset_name,
                    json.dumps(schedule) if schedule else None,
                    json.dumps(st.session_state.source_config),
                )
            )
            
            # Create and start pipeline run
            run_id = get_next_pipeline_run_id()
            insert_run_query = """
            INSERT INTO pipeline_runs (id, pipeline_id, status, start_time, end_time, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            execute_query(
                insert_run_query,
                params=(run_id, pipeline_id, "pending", datetime.now(), None, None)
            )
            
            # Navigate to Pipeline Runs page
            st.success("Pipeline created and starting run!")
            time.sleep(1)  # Brief pause for user feedback
            st.switch_page(f"pages/pipeline_runs.py?run_id={run_id}")


if __name__ == "__main__":
    pipeline_creator_page()
