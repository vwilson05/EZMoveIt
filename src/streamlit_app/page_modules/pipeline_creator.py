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

CONFIG_DIR = "config"
os.makedirs(CONFIG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_verified_dlt_sources():
    return [
        "REST API (Public)",
        "REST API (Private)",
        "microsoft_sqlserver",
        "oracle",
    ]


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
            source_options = ["REST API - Public", "REST API - Private", "Database", "File"]
            st.session_state.selected_source = st.selectbox(
                "Select Source Type",
                source_options
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
                    ["None", "API Key", "OAuth2", "Basic Auth"]
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
                
                # Incremental Load Settings
                incremental_load = st.checkbox("Enable Incremental Load")
                if incremental_load:
                    incremental_field = st.text_input("Incremental Field Name", placeholder="e.g., updated_at")
                    st.session_state.source_config["incremental_load"] = {
                        "enabled": True,
                        "field": incremental_field
                    }
            
            elif st.session_state.selected_source == "Database":
                # Database connection details
                db_type = st.selectbox("Database Type", ["PostgreSQL", "MySQL", "SQLite"])
                host = st.text_input("Host", placeholder="localhost")
                port = st.number_input("Port", min_value=1, max_value=65535, value=5432)
                database = st.text_input("Database Name")
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                
                st.session_state.source_config = {
                    "db_type": db_type,
                    "host": host,
                    "port": port,
                    "database": database,
                    "username": username,
                    "password": password
                }
            
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
        st.session_state.start_time_val = st.time_input(
            "Start Time",
            value=st.session_state.start_time_val.time()
        )
    elif st.session_state.schedule_option == "Weekly":
        st.session_state.start_time_val = st.time_input(
            "Start Time",
            value=st.session_state.start_time_val.time()
        )
        weekday = st.selectbox(
            "Day of Week",
            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        )
    
    # Create Pipeline Button
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Create Pipeline", type="primary"):
            if not st.session_state.pipeline_name:
                st.error("Please enter a pipeline name")
            elif not st.session_state.source_url:
                st.error("Please enter a source URL")
            elif not st.session_state.source_config:
                st.error("Please configure the source")
            elif not st.session_state.dataset_name:
                st.error("Please enter a dataset name")
            elif not st.session_state.target_table:
                st.error("Please enter a target table name")
            else:
                try:
                    # Insert pipeline record
                    query = """
                    INSERT INTO pipelines (
                        id, name, source_url, target_table, dataset_name, schedule
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """
                    schedule = None
                    if st.session_state.schedule_option != "Manual":
                        schedule = {
                            "type": st.session_state.schedule_option.lower(),
                            "interval_minutes": st.session_state.interval_minutes,
                            "start_time": st.session_state.start_time_val.strftime("%H:%M:%S")
                        }
                        if st.session_state.schedule_option == "Weekly":
                            schedule["weekday"] = weekday
                    
                    execute_query(
                        query,
                        params=(
                            get_next_pipeline_id(),
                            st.session_state.pipeline_name,
                            st.session_state.source_url,
                            st.session_state.target_table,
                            st.session_state.dataset_name,
                            json.dumps(schedule) if schedule else None
                        )
                    )
                    
                    st.success("Pipeline created successfully!")
                    
                    # Clear form data
                    for key in ['pipeline_name', 'source_url', 'target_table', 'dataset_name',
                              'schedule_option', 'start_time_val', 'interval_minutes']:
                        st.session_state[key] = ""
                    st.session_state.source_config = {}
                    
                except Exception as e:
                    st.error(f"Error creating pipeline: {str(e)}")
    
    with col2:
        if st.button("🚀 Create & Run Pipeline", type="secondary"):
            if not st.session_state.pipeline_name:
                st.error("Please enter a pipeline name")
            elif not st.session_state.source_url:
                st.error("Please enter a source URL")
            elif not st.session_state.source_config:
                st.error("Please configure the source")
            elif not st.session_state.dataset_name:
                st.error("Please enter a dataset name")
            elif not st.session_state.target_table:
                st.error("Please enter a target table name")
            else:
                try:
                    # Capture values before clearing session state
                    pipeline_name = st.session_state.pipeline_name
                    dataset_name = st.session_state.dataset_name
                    target_table = st.session_state.target_table
                    snowflake_creds = st.session_state.get("snowflake_creds")
                    
                    # Insert pipeline record
                    query = """
                    INSERT INTO pipelines (
                        id, name, source_url, target_table, dataset_name, schedule
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """
                    schedule = None
                    if st.session_state.schedule_option != "Manual":
                        schedule = {
                            "type": st.session_state.schedule_option.lower(),
                            "interval_minutes": st.session_state.interval_minutes,
                            "start_time": st.session_state.start_time_val.strftime("%H:%M:%S")
                        }
                        if st.session_state.schedule_option == "Weekly":
                            schedule["weekday"] = weekday
                    
                    execute_query(
                        query,
                        params=(
                            get_next_pipeline_id(),
                            pipeline_name,
                            st.session_state.source_url,
                            target_table,
                            dataset_name,
                            json.dumps(schedule) if schedule else None
                        )
                    )
                    
                    st.success("Pipeline created successfully!")
                    
                    # Create containers for different stages with custom styling
                    st.markdown("""
                        <style>
                        .stProgress > div > div > div > div {
                            background-color: #1f77b4;
                        }
                        .stProgress > div > div > div > div:nth-child(2) {
                            background-color: #2ca02c;
                        }
                        .stProgress > div > div > div > div:nth-child(3) {
                            background-color: #ff7f0e;
                        }
                        </style>
                    """, unsafe_allow_html=True)
                    
                    # Create expandable section for pipeline progress
                    with st.expander("Pipeline Progress", expanded=True):
                        # Extract stage
                        extract_container = st.empty()
                        extract_status_text = st.empty()
                        
                        # Normalize stage
                        normalize_container = st.empty()
                        normalize_status_text = st.empty()
                        
                        # Load stage
                        load_container = st.empty()
                        load_status_text = st.empty()
                        
                        # Overall status and metrics
                        status_container = st.empty()
                        metrics_container = st.empty()
                        
                        result_container = {"status": "Pipeline started...", "result": None}
                        
                        def run_pipeline_thread():
                            result_container["status"] = "Pipeline started..."
                            res = run_pipeline_with_creds(pipeline_name, dataset_name, target_table, snowflake_creds)
                            if res is not None:
                                result_container["status"] = f"Pipeline completed: {res} rows loaded."
                                result_container["result"] = res
                            else:
                                result_container["status"] = "Pipeline failed. Check logs."
                        
                        pipeline_thread = threading.Thread(target=run_pipeline_thread, daemon=True)
                        pipeline_thread.start()
                        
                        # Update UI with progress
                        while pipeline_thread.is_alive():
                            # Get current run status
                            run_status = execute_query(
                                """
                                SELECT status, extract_status, normalize_status, load_status, 
                                       duration, rows_processed
                                FROM pipeline_runs 
                                WHERE pipeline_name = ? 
                                ORDER BY start_time DESC 
                                LIMIT 1
                                """,
                                (pipeline_name,),
                                fetch=True
                            )
                            
                            if run_status:
                                status, extract_status, normalize_status, load_status, duration, rows = run_status[0]
                                
                                # Update extract progress
                                extract_container.progress(100 if extract_status == 'completed' else 50)
                                extract_status_text.markdown(f"**Extract:** {extract_status}")
                                
                                # Update normalize progress
                                normalize_container.progress(100 if normalize_status == 'completed' else 50)
                                normalize_status_text.markdown(f"**Normalize:** {normalize_status}")
                                
                                # Update load progress
                                load_container.progress(100 if load_status == 'completed' else 50)
                                load_status_text.markdown(f"**Load:** {load_status}")
                                
                                # Update overall status
                                status_container.info(f"**Status:** {status}")
                                
                                # Update metrics if available
                                if duration and rows:
                                    metrics_container.markdown(f"""
                                        ### 📊 Current Metrics
                                        - **Duration:** {duration:.2f} seconds
                                        - **Rows Processed:** {rows:,}
                                    """)
                            
                            time.sleep(0.5)
                        
                        # Final update
                        if result_container["result"] is not None:
                            extract_status_text.markdown("✅ **Extraction complete**")
                            normalize_status_text.markdown("✅ **Normalization complete**")
                            load_status_text.markdown("✅ **Load complete**")
                            status_container.success(result_container["status"])
                            
                            # Show final metrics
                            final_metrics = execute_query(
                                """
                                SELECT duration, rows_processed
                                FROM pipeline_runs 
                                WHERE pipeline_name = ? 
                                ORDER BY start_time DESC 
                                LIMIT 1
                                """,
                                (pipeline_name,),
                                fetch=True
                            )
                            
                            if final_metrics:
                                duration, rows = final_metrics[0]
                                metrics_container.markdown(f"""
                                    ### 📊 Final Metrics
                                    - **Duration:** {duration:.2f} seconds
                                    - **Rows Processed:** {rows:,}
                                    - **Rows/Second:** {rows/duration:.2f}
                                """)
                        else:
                            extract_status_text.markdown("❌ **Extraction failed**")
                            normalize_status_text.markdown("❌ **Normalization failed**")
                            load_status_text.markdown("❌ **Load failed**")
                            status_container.error(result_container["status"])
                    
                    # Clear form data
                    for key in ['pipeline_name', 'source_url', 'target_table', 'dataset_name',
                              'schedule_option', 'start_time_val', 'interval_minutes']:
                        st.session_state[key] = ""
                    st.session_state.source_config = {}
                    
                except Exception as e:
                    st.error(f"Error creating pipeline: {str(e)}")

    st.subheader("📜 Existing Pipelines")
    pipelines = execute_query(
        "SELECT id, name, source_url, target_table, dataset_name, schedule, last_run_status FROM pipelines ORDER BY id DESC",
        fetch=True,
    )
    if pipelines:
        for row in pipelines:
            pid, pname, src, tgt, ds, schedule, status = row
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**ID:** `{pid}` | **Name:** `{pname}`")
                st.markdown(f"**Source URL:** `{src}`")
                st.markdown(f"**Target Table:** `{tgt}` | **Schema:** `{ds}`")
                st.markdown(f"**Schedule:** `{schedule if schedule else 'One-Time Run'}`")
                st.markdown(f"**Last Run Status:** `{status}`")
            with col2:
                if status == "running":
                    st.button("Running...", key=f"disabled_{pid}", disabled=True)
                else:
                    if st.button(f"Run {pname}", key=f"trigger_{pid}"):
                        creds = st.session_state.get("snowflake_creds")
                        if not creds:
                            st.error("No Snowflake credentials found. Please enter them above.")
                        else:
                            # Create containers for different stages with custom styling
                            st.markdown("""
                                <style>
                                .stProgress > div > div > div > div {
                                    background-color: #1f77b4;
                                }
                                .stProgress > div > div > div > div:nth-child(2) {
                                    background-color: #2ca02c;
                                }
                                .stProgress > div > div > div > div:nth-child(3) {
                                    background-color: #ff7f0e;
                                }
                                </style>
                            """, unsafe_allow_html=True)
                                
                            # Create expandable section for pipeline progress
                            with st.expander("Pipeline Progress", expanded=True):
                                # Extract stage
                                extract_container = st.empty()
                                extract_status_text = st.empty()
                                
                                # Normalize stage
                                normalize_container = st.empty()
                                normalize_status_text = st.empty()
                                
                                # Load stage
                                load_container = st.empty()
                                load_status_text = st.empty()
                                
                                # Overall status and metrics
                                status_container = st.empty()
                                metrics_container = st.empty()
                                
                                result_container = {"status": "Pipeline started...", "result": None}

                                def run_pipeline_thread():
                                    result_container["status"] = "Pipeline started..."
                                    res = run_pipeline_with_creds(pname, ds, tgt, creds)
                                    if res is not None:
                                        result_container["status"] = f"Pipeline completed: {res} rows loaded."
                                        result_container["result"] = res
                                    else:
                                        result_container["status"] = "Pipeline failed. Check logs."

                                pipeline_thread = threading.Thread(target=run_pipeline_thread, daemon=True)
                                pipeline_thread.start()

                                # Update UI with progress
                                while pipeline_thread.is_alive():
                                    # Get current run status
                                    run_status = execute_query(
                                        """
                                        SELECT status, extract_status, normalize_status, load_status, 
                                               duration, rows_processed
                                        FROM pipeline_runs 
                                        WHERE pipeline_name = ? 
                                        ORDER BY start_time DESC 
                                        LIMIT 1
                                        """,
                                        (pname,),
                                        fetch=True
                                    )
                                    
                                    if run_status:
                                        status, extract_status, normalize_status, load_status, duration, rows = run_status[0]
                                        
                                        # Update extract progress
                                        extract_container.progress(100 if extract_status == 'completed' else 50)
                                        extract_status_text.markdown(f"**Extract:** {extract_status}")
                                        
                                        # Update normalize progress
                                        normalize_container.progress(100 if normalize_status == 'completed' else 50)
                                        normalize_status_text.markdown(f"**Normalize:** {normalize_status}")
                                        
                                        # Update load progress
                                        load_container.progress(100 if load_status == 'completed' else 50)
                                        load_status_text.markdown(f"**Load:** {load_status}")
                                        
                                        # Update overall status
                                        status_container.info(f"**Status:** {status}")
                                        
                                        # Update metrics if available
                                        if duration and rows:
                                            metrics_container.markdown(f"""
                                                ### 📊 Current Metrics
                                                - **Duration:** {duration:.2f} seconds
                                                - **Rows Processed:** {rows:,}
                                            """)
                                    
                                    time.sleep(0.5)

                                # Final update
                                if result_container["result"] is not None:
                                    extract_status_text.markdown("✅ **Extraction complete**")
                                    normalize_status_text.markdown("✅ **Normalization complete**")
                                    load_status_text.markdown("✅ **Load complete**")
                                    status_container.success(result_container["status"])
                                    
                                    # Show final metrics
                                    final_metrics = execute_query(
                                        """
                                        SELECT duration, rows_processed
                                        FROM pipeline_runs 
                                        WHERE pipeline_name = ? 
                                        ORDER BY start_time DESC 
                                        LIMIT 1
                                        """,
                                        (pname,),
                                        fetch=True
                                    )
                                    
                                    if final_metrics:
                                        duration, rows = final_metrics[0]
                                        metrics_container.markdown(f"""
                                            ### 📊 Final Metrics
                                            - **Duration:** {duration:.2f} seconds
                                            - **Rows Processed:** {rows:,}
                                            - **Rows/Second:** {rows/duration:.2f}
                                        """)
                                else:
                                    extract_status_text.markdown("❌ **Extraction failed**")
                                    normalize_status_text.markdown("❌ **Normalization failed**")
                                    load_status_text.markdown("❌ **Load failed**")
                                    status_container.error(result_container["status"])
                    st.markdown("---")
    else:
        st.info("No pipelines found. Create one above!")


if __name__ == "__main__":
    pipeline_creator_page()
