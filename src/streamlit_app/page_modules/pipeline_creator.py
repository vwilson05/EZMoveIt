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


# New function to get metadata from config
def get_metadata_configs(filter_params=None):
    """Get metadata configurations with optional filtering."""
    base_query = """
    SELECT 
        id, source_type, driver_type, logical_name, hostname, port,
        database_name, schema_name, table_name, source_url, endpoint,
        load_type, primary_key, delta_column, delta_value
    FROM metadata_config
    """
    
    where_clauses = []
    params = []
    
    if filter_params:
        if filter_params.get('source_type'):
            where_clauses.append("source_type = ?")
            params.append(filter_params['source_type'])
        
        if filter_params.get('logical_name'):
            where_clauses.append("logical_name = ?")
            params.append(filter_params['logical_name'])
            
        if filter_params.get('database_name'):
            where_clauses.append("database_name = ?")
            params.append(filter_params['database_name'])
            
        if filter_params.get('schema_name'):
            where_clauses.append("schema_name = ?")
            params.append(filter_params['schema_name'])
            
        if filter_params.get('load_type'):
            where_clauses.append("load_type = ?")
            params.append(filter_params['load_type'])
    
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
        
    base_query += " ORDER BY source_type, logical_name, database_name, schema_name, table_name"
    
    return execute_query(base_query, params=params if params else None, fetch=True)


# New function to generate configuration JSON for a metadata-driven pipeline
def generate_pipeline_config(selected_metadata_ids, load_type):
    """Generate a DLT-compatible configuration for selected metadata objects."""
    if not selected_metadata_ids:
        return None
        
    # Get the full metadata records for the selected IDs
    id_placeholders = ", ".join(["?" for _ in selected_metadata_ids])
    query = f"""
    SELECT 
        id, source_type, driver_type, logical_name, hostname, port,
        database_name, schema_name, table_name, source_url, endpoint,
        load_type, primary_key, delta_column, delta_value
    FROM metadata_config
    WHERE id IN ({id_placeholders})
    ORDER BY source_type, logical_name, database_name, schema_name, table_name
    """
    
    metadata_records = execute_query(query, params=selected_metadata_ids, fetch=True)
    
    # Group records by source connection
    source_groups = {}
    for record in metadata_records:
        # Create a key that uniquely identifies a source connection
        if record[1] in ["SQL Server", "Oracle"]:  # Database sources
            source_key = f"{record[1]}_{record[4]}_{record[6]}"  # source_type_hostname_database
        else:  # API sources
            source_key = f"{record[1]}_{record[9]}"  # source_type_url
            
        if source_key not in source_groups:
            source_groups[source_key] = {
                "source_type": record[1],
                "driver_type": record[2],
                "logical_name": record[3],
                "hostname": record[4],
                "port": record[5],
                "database_name": record[6],
                "schema_name": record[7],
                "source_url": record[9],
                "endpoint": record[10],
                "load_type": load_type,  # Use the pipeline-level load type
                "tables": []
            }
        
        # Add table information
        source_groups[source_key]["tables"].append({
            "table_name": record[8],
            "primary_key": record[12],
            "delta_column": record[13],
            "delta_value": record[14]
        })
    
    # Generate configuration for each source group
    configs = []
    for source_key, group in source_groups.items():
        if "SQL Server" in group["source_type"] or "Oracle" in group["source_type"]:
            # Database source configuration
            config = {
                "db_type": group["source_type"].lower().replace(" ", "_"),
                "mode": "sql_database" if len(group["tables"]) > 1 else "sql_table",
                "driver": group["driver_type"] if group["source_type"] == "SQL Server" else None,
                "host": group["hostname"],
                "port": group["port"],
                "database": group["database_name"],
                "schema": group["schema_name"],
                "incremental_type": group["load_type"].upper(),
                "use_parallel": True,
                "chunk_size": 100000
            }
            
            if config["mode"] == "sql_table":
                # Single table mode
                config["table"] = group["tables"][0]["table_name"]
                if group["load_type"] == "incremental":
                    config["primary_key"] = group["tables"][0]["primary_key"]
                    config["delta_column"] = group["tables"][0]["delta_column"]
                    config["delta_value"] = group["tables"][0]["delta_value"]
            else:
                # Multiple tables mode
                config["tables"] = [t["table_name"] for t in group["tables"]]
                
                # For incremental loads, we need to handle the incremental settings
                if group["load_type"] == "incremental":
                    # Use the first table's settings as defaults
                    config["primary_key"] = group["tables"][0]["primary_key"]
                    config["delta_column"] = group["tables"][0]["delta_column"]
                    config["delta_value"] = group["tables"][0]["delta_value"]
        else:
            # API source configuration
            config = {
                "source_type": "rest_api",
                "auth_type": "none",  # Default, modify as needed
                "pagination": {
                    "type": "none",
                    "page_size": 100
                },
                "incremental_type": group["load_type"].upper()
            }
            
            if group["load_type"] == "incremental" and group["tables"]:
                config["incremental_load"] = {
                    "enabled": True,
                    "field": group["tables"][0]["delta_column"]
                }
        
        configs.append(config)
    
    # For simplicity, we'll use the first config if there's only one source group
    if len(configs) == 1:
        return configs[0]
    
    # For multiple source groups, we'll need to handle this differently
    # One approach is to use the first config and note that multiple pipelines may be needed
    return configs[0]


def pipeline_creator_page():
    st.title("➕ Create Pipeline")
    st.caption("Create a new data pipeline to extract, transform, and load data.")
    
    # Create tabs for different creation methods
    tabs = st.tabs(["Manual Configuration", "Metadata-Driven Configuration"])
    
    # Tab 1: Manual Configuration (existing functionality)
    with tabs[0]:
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
            placeholder="e.g., API-private-spotify-playlist",
            key="manual_pipeline_name"
        )
        
        # Dataset Name
        st.session_state.dataset_name = st.text_input(
            "Dataset Name",
            value=st.session_state.dataset_name,
            placeholder="e.g., CUSTOMER_DATA",
            key="manual_dataset_name"
        )
        
        # Target Table Name
        st.session_state.target_table = st.text_input(
            "Target Table Name",
            value=st.session_state.target_table,
            placeholder="e.g., CUSTOMER_ORDERS",
            key="manual_target_table"
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
        
        # Schedule Configuration
        st.markdown("---")
        st.subheader("Schedule Configuration")
        
        schedule_option = st.radio(
            "Schedule Type",
            ["Manual", "Interval", "Daily", "Weekly"],
            horizontal=True,
            key="manual_schedule_option"
        )
        
        if schedule_option == "Interval":
            interval_minutes = st.number_input(
                "Interval (minutes)",
                min_value=1,
                max_value=1440,
                value=60,
                key="manual_interval_minutes"
            )
        elif schedule_option == "Daily":
            start_time_val = st.time_input(
                "Start Time",
                value=datetime.now().time(),
                key="manual_start_time"
            )
        elif schedule_option == "Weekly":
            col1, col2 = st.columns(2)
            with col1:
                start_time_val = st.time_input(
                    "Start Time",
                    value=datetime.now().time(),
                    key="manual_weekly_start_time"
                )
            with col2:
                weekday = st.selectbox(
                    "Day of Week",
                    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                    key="manual_weekday"
                )
        
        # Prepare schedule JSON
        schedule = None
        if schedule_option != "Manual":
            schedule = {
                "type": schedule_option.lower(),
                "interval_minutes": interval_minutes if schedule_option == "Interval" else None,
                "start_time": start_time_val.strftime("%H:%M:%S") if schedule_option in ["Daily", "Weekly"] else None,
                "weekday": weekday if schedule_option == "Weekly" else None
            }
        
        # Create Pipeline buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Create Pipeline", type="primary", use_container_width=True, key="manual_create_btn"):
                if not st.session_state.pipeline_name:
                    st.error("Please enter a pipeline name.")
                elif not st.session_state.dataset_name:
                    st.error("Please enter a dataset name.")
                elif not st.session_state.target_table:
                    st.error("Please enter a target table name.")
                else:
                    try:
                        # Save configuration to file
                        save_source_config(st.session_state.pipeline_name, st.session_state.source_config)
                        
                        # Insert pipeline into database
                        pipeline_id = get_next_pipeline_id()
                        insert_query = """
                        INSERT INTO pipelines (
                            id, 
                            name, 
                            source_url, 
                            target_table, 
                            dataset_name, 
                            schedule, 
                            source_config,
                            metadata_selection,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """
                        
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
                                None  # metadata_selection is None for manual configuration
                            )
                        )
                        
                        st.success("Pipeline created successfully!")
                        time.sleep(1)  # Brief pause for user feedback
                        st.session_state.current_page = "Pipeline List"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating pipeline: {str(e)}")
        
        with col2:
            if st.button("Create + Run Pipeline", type="primary", use_container_width=True, key="manual_create_run_btn"):
                if not st.session_state.pipeline_name:
                    st.error("Please enter a pipeline name.")
                elif not st.session_state.dataset_name:
                    st.error("Please enter a dataset name.")
                elif not st.session_state.target_table:
                    st.error("Please enter a target table name.")
                else:
                    try:
                        # Save configuration to file
                        save_source_config(st.session_state.pipeline_name, st.session_state.source_config)
                        
                        # Insert pipeline into database
                        pipeline_id = get_next_pipeline_id()
                        insert_query = """
                        INSERT INTO pipelines (
                            id, 
                            name, 
                            source_url, 
                            target_table, 
                            dataset_name, 
                            schedule, 
                            source_config,
                            metadata_selection,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """
                        
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
                                None  # metadata_selection is None for manual configuration
                            )
                        )
                        
                        # Create and start pipeline run
                        run_id = get_next_pipeline_run_id()
                        insert_run_query = """
                        INSERT INTO pipeline_runs (id, pipeline_id, pipeline_name, status, start_time, end_time, error_message)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        execute_query(
                            insert_run_query,
                            params=(
                                run_id,
                                pipeline_id,
                                st.session_state.pipeline_name,
                                "queued",
                                datetime.now(),
                                None,
                                None
                            )
                        )
                        
                        # Start the pipeline run in a separate thread
                        thread = threading.Thread(
                            target=run_pipeline_with_creds,
                            args=(
                                st.session_state.pipeline_name,
                                st.session_state.dataset_name,
                                st.session_state.target_table,
                                load_snowflake_credentials()
                            )
                        )
                        thread.start()
                        
                        st.success("Pipeline created and started successfully!")
                        time.sleep(1)  # Brief pause for user feedback
                        st.session_state.current_page = "Pipeline List"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating and running pipeline: {str(e)}")

    # Tab 2: Metadata-Driven Configuration (new functionality)
    with tabs[1]:
        st.subheader("Create Pipeline from Metadata")
        st.write("Select objects from the metadata configuration to include in your pipeline.")
        
        # Pipeline basics
        pipeline_name = st.text_input(
            "Pipeline Name", 
            placeholder="e.g., METADATA-Customer-Orders",
            key="metadata_pipeline_name"
        )
        
        dataset_name = st.text_input(
            "Dataset Name",
            placeholder="e.g., CUSTOMER_DATA",
            key="metadata_dataset_name"
        )
        
        # Set target table name
        target_table = st.text_input(
            "Target Table Name",
            placeholder="e.g., CUSTOMER_ORDERS",
            key="metadata_target_table"
        )
        
        # Load Type Selection - this applies to all objects in the pipeline
        load_type = st.selectbox(
            "Load Type", 
            ["FULL", "INCREMENTAL", "CDC"],
            help="Select the load type for this pipeline. All objects must use the same load strategy."
        )
        
        # Selection mode - IMPORTANT: this should be before filtering
        selection_mode = st.radio(
            "Selection Mode",
            ["Filter-Based", "Explicit Selection"],
            horizontal=True,
            help="Filter-Based automatically includes new objects matching criteria. Explicit Selection includes only specifically selected objects."
        )
        
        # Filtering options for metadata
        st.subheader("Filter Metadata Objects")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Get distinct source types
            source_types = execute_query(
                "SELECT DISTINCT source_type FROM metadata_config ORDER BY source_type",
                fetch=True
            )
            selected_source_type = st.selectbox(
                "Source Type",
                options=[None] + [s[0] for s in source_types],
                format_func=lambda x: "All Source Types" if x is None else x
            )
            
            # Get distinct logical names
            logical_names = execute_query(
                "SELECT DISTINCT logical_name FROM metadata_config ORDER BY logical_name",
                fetch=True
            )
            selected_logical_name = st.selectbox(
                "Logical Name",
                options=[None] + [ln[0] for ln in logical_names],
                format_func=lambda x: "All Logical Names" if x is None else x
            )
        
        with col2:
            # Get distinct database names
            db_names = execute_query(
                "SELECT DISTINCT database_name FROM metadata_config WHERE database_name IS NOT NULL ORDER BY database_name",
                fetch=True
            )
            selected_db_name = st.selectbox(
                "Database Name",
                options=[None] + [db[0] for db in db_names],
                format_func=lambda x: "All Databases" if x is None else x
            )
            
            # Get distinct schema names
            schema_names = execute_query(
                "SELECT DISTINCT schema_name FROM metadata_config WHERE schema_name IS NOT NULL ORDER BY schema_name",
                fetch=True
            )
            selected_schema_name = st.selectbox(
                "Schema Name",
                options=[None] + [s[0] for s in schema_names],
                format_func=lambda x: "All Schemas" if x is None else x
            )
        
        # Build filter parameters
        filter_params = {}
        if selected_source_type:
            filter_params['source_type'] = selected_source_type
        if selected_logical_name:
            filter_params['logical_name'] = selected_logical_name
        if selected_db_name:
            filter_params['database_name'] = selected_db_name
        if selected_schema_name:
            filter_params['schema_name'] = selected_schema_name
        
        # Apply load type filter
        filter_params['load_type'] = load_type
        
        # Get filtered metadata objects
        metadata_objects = get_metadata_configs(filter_params)
        
        # For filter-based selection
        if selection_mode == "Filter-Based":
            st.info("Filter-Based selection will automatically include any new objects that match your filter criteria when the pipeline runs.")
            
            # Store the filter criteria rather than specific object IDs
            selection_criteria = {
                "type": "filter",
                "load_type": load_type,
                "filters": {
                    "source_type": selected_source_type,
                    "logical_name": selected_logical_name,
                    "database_name": selected_db_name,
                    "schema_name": selected_schema_name
                }
            }
            
            # Show the filtered objects as a preview
            if metadata_objects:
                # Convert to dataframe for display
                df = pd.DataFrame(metadata_objects, columns=[
                    "ID", "Source Type", "Driver Type", "Logical Name", "Hostname", "Port",
                    "Database Name", "Schema Name", "Table Name", "Source URL", "Endpoint",
                    "Load Type", "Primary Key", "Delta Column", "Delta Value"
                ])
                
                st.write(f"Preview: {len(metadata_objects)} objects currently match these criteria")
                st.dataframe(df, hide_index=True)
                
                if len(metadata_objects) > 0:
                    st.success(f"{len(metadata_objects)} objects will be included in this pipeline")
                else:
                    st.warning("No objects match the current filter criteria. Adjust your filters to include some objects.")
                    selection_criteria = None  # Don't allow creation with no matching objects
            else:
                st.warning("No objects match the current filter criteria. Adjust your filters or add objects in the Metadata Configuration page.")
                selection_criteria = None
        
        # For explicit selection
        else:  # Explicit Selection mode
            if metadata_objects:
                # Convert to dataframe for display and selection
                df = pd.DataFrame(metadata_objects, columns=[
                    "ID", "Source Type", "Driver Type", "Logical Name", "Hostname", "Port",
                    "Database Name", "Schema Name", "Table Name", "Source URL", "Endpoint",
                    "Load Type", "Primary Key", "Delta Column", "Delta Value"
                ])
                
                # Add selection column
                df['Select'] = False
                
                # Display the dataframe with selection checkboxes
                st.subheader("Select Objects to Include")
                selected_df = st.data_editor(
                    df,
                    hide_index=True,
                    column_config={
                        "Select": st.column_config.CheckboxColumn(
                            "Select",
                            help="Select objects to include in pipeline"
                        ),
                        "ID": st.column_config.NumberColumn(
                            "ID",
                            disabled=True
                        )
                    },
                    disabled=["ID", "Source Type", "Driver Type", "Logical Name", "Hostname", "Port",
                            "Database Name", "Schema Name", "Table Name", "Source URL", "Endpoint",
                            "Load Type", "Primary Key", "Delta Column", "Delta Value"]
                )
                
                # Get selected object IDs
                selected_ids = selected_df[selected_df['Select']]['ID'].tolist()
                
                if selected_ids:
                    # Store explicit selection criteria
                    selection_criteria = {
                        "type": "explicit",
                        "object_ids": selected_ids,
                        "load_type": load_type
                    }
                    
                    st.success(f"Selected {len(selected_ids)} objects for the pipeline.")
                else:
                    st.warning("Please select at least one object to include in the pipeline.")
                    selection_criteria = None  # Don't allow creation with no selected objects
            else:
                st.warning("No objects match the current filter criteria. Adjust your filters to see objects for selection.")
                selection_criteria = None
        
        # Schedule Configuration
        st.markdown("---")
        st.subheader("Schedule Configuration")
        
        schedule_option = st.radio(
            "Schedule Type",
            ["Manual", "Interval", "Daily", "Weekly"],
            horizontal=True,
            key="metadata_schedule_option"
        )
        
        if schedule_option == "Interval":
            interval_minutes = st.number_input(
                "Interval (minutes)",
                min_value=1,
                max_value=1440,
                value=60,
                key="metadata_interval_minutes"
            )
        elif schedule_option == "Daily":
            start_time_val = st.time_input(
                "Start Time",
                value=datetime.now().time(),
                key="metadata_start_time"
            )
        elif schedule_option == "Weekly":
            col1, col2 = st.columns(2)
            with col1:
                start_time_val = st.time_input(
                    "Start Time",
                    value=datetime.now().time(),
                    key="metadata_weekly_start_time"
                )
            with col2:
                weekday = st.selectbox(
                    "Day of Week",
                    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                    key="metadata_weekday"
                )
        
        # Prepare schedule JSON
        schedule = None
        if schedule_option != "Manual":
            schedule = {
                "type": schedule_option.lower(),
                "interval_minutes": interval_minutes if schedule_option == "Interval" else None,
                "start_time": start_time_val.strftime("%H:%M:%S") if schedule_option in ["Daily", "Weekly"] else None,
                "weekday": weekday if schedule_option == "Weekly" else None
            }
        
        # Create Pipeline buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Create Pipeline", type="primary", use_container_width=True, key="metadata_create_btn"):
                if not pipeline_name:
                    st.error("Please enter a pipeline name.")
                elif not dataset_name:
                    st.error("Please enter a dataset name.")
                elif not target_table:
                    st.error("Please enter a target table name.")
                elif not selection_criteria:
                    st.error("Please define valid selection criteria (filter or explicit selection).")
                else:
                    try:
                        # Generate pipeline configuration
                        if selection_mode == "Filter-Based":
                            # For filter-based, use the current matching objects to create initial config
                            if metadata_objects:
                                pipeline_config = generate_pipeline_config(
                                    [obj[0] for obj in metadata_objects],  # Use IDs of all matching objects
                                    load_type
                                )
                            else:
                                st.error("No objects match your filter criteria. Cannot create pipeline.")
                                return
                        else:
                            # For explicit selection, use the selected IDs
                            pipeline_config = generate_pipeline_config(selection_criteria["object_ids"], load_type)
                        
                        if not pipeline_config:
                            st.error("Failed to generate pipeline configuration.")
                            return
                        
                        # Determine appropriate source_url
                        if pipeline_config.get("db_type", "").lower() in ["sql_server", "microsoft_sqlserver"]:
                            source_url = f"microsoft_sqlserver://{pipeline_config['host']}:{pipeline_config['port']}/{pipeline_config['database']}"
                        elif pipeline_config.get("db_type", "").lower() == "oracle":
                            source_url = f"oracle://{pipeline_config['host']}:{pipeline_config['port']}/{pipeline_config['database']}"
                        else:
                            # For API and other sources
                            source_url = pipeline_config.get("source_url", "")
                        
                        # Save configuration to file
                        save_source_config(pipeline_name, pipeline_config)
                        
                        # Insert pipeline into database
                        pipeline_id = get_next_pipeline_id()
                        insert_query = """
                        INSERT INTO pipelines (
                            id, 
                            name, 
                            source_url, 
                            target_table, 
                            dataset_name, 
                            schedule, 
                            source_config,
                            metadata_selection,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """
                        
                        execute_query(
                            insert_query,
                            params=(
                                pipeline_id,
                                pipeline_name,
                                source_url,
                                target_table,
                                dataset_name,
                                json.dumps(schedule) if schedule else None,
                                json.dumps(pipeline_config),
                                json.dumps(selection_criteria)
                            )
                        )
                        
                        st.success("Pipeline created successfully!")
                        time.sleep(1)  # Brief pause for user feedback
                        st.session_state.current_page = "Pipeline List"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating pipeline: {str(e)}")
        
        with col2:
            if st.button("Create + Run Pipeline", type="primary", use_container_width=True, key="metadata_create_run_btn"):
                if not pipeline_name:
                    st.error("Please enter a pipeline name.")
                elif not dataset_name:
                    st.error("Please enter a dataset name.")
                elif not target_table:
                    st.error("Please enter a target table name.")
                elif not selection_criteria:
                    st.error("Please define valid selection criteria (filter or explicit selection).")
                else:
                    try:
                        # Generate pipeline configuration
                        if selection_mode == "Filter-Based":
                            # For filter-based, use the current matching objects to create initial config
                            if metadata_objects:
                                pipeline_config = generate_pipeline_config(
                                    [obj[0] for obj in metadata_objects],  # Use IDs of all matching objects
                                    load_type
                                )
                            else:
                                st.error("No objects match your filter criteria. Cannot create pipeline.")
                                return
                        else:
                            # For explicit selection, use the selected IDs
                            pipeline_config = generate_pipeline_config(selection_criteria["object_ids"], load_type)
                        
                        if not pipeline_config:
                            st.error("Failed to generate pipeline configuration.")
                            return
                        
                        # Determine appropriate source_url
                        if pipeline_config.get("db_type", "").lower() in ["sql_server", "microsoft_sqlserver"]:
                            source_url = f"microsoft_sqlserver://{pipeline_config['host']}:{pipeline_config['port']}/{pipeline_config['database']}"
                        elif pipeline_config.get("db_type", "").lower() == "oracle":
                            source_url = f"oracle://{pipeline_config['host']}:{pipeline_config['port']}/{pipeline_config['database']}"
                        else:
                            # For API and other sources
                            source_url = pipeline_config.get("source_url", "")
                        
                        # Save configuration to file
                        save_source_config(pipeline_name, pipeline_config)
                        
                        # Insert pipeline into database
                        pipeline_id = get_next_pipeline_id()
                        insert_query = """
                        INSERT INTO pipelines (
                            id, 
                            name, 
                            source_url, 
                            target_table, 
                            dataset_name, 
                            schedule, 
                            source_config,
                            metadata_selection,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """
                        
                        execute_query(
                            insert_query,
                            params=(
                                pipeline_id,
                                pipeline_name,
                                source_url,
                                target_table,
                                dataset_name,
                                json.dumps(schedule) if schedule else None,
                                json.dumps(pipeline_config),
                                json.dumps(selection_criteria)
                            )
                        )
                        
                        # Create and start pipeline run
                        run_id = get_next_pipeline_run_id()
                        insert_run_query = """
                        INSERT INTO pipeline_runs (id, pipeline_id, pipeline_name, status, start_time, end_time, error_message)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        execute_query(
                            insert_run_query,
                            params=(run_id, pipeline_id, pipeline_name, "pending", datetime.now(), None, None)
                        )
                        
                        # Navigate to Pipeline Runs page
                        st.success("Pipeline created and starting run!")
                        time.sleep(1)  # Brief pause for user feedback
                        st.switch_page(f"pages/pipeline_runs.py?run_id={run_id}")
                    except Exception as e:
                        st.error(f"Error creating pipeline: {str(e)}")


if __name__ == "__main__":
    pipeline_creator_page()
