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


def save_source_config(source_key, config_data):
    config_path = os.path.join(CONFIG_DIR, f"{source_key.replace(' ', '_').lower()}_config.json")
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=2, default=str)


@dlt.source
def selected_tables_source(engine, schema: str, table_names: list[str]):
    logging.info(f"Creating source from tables: {table_names} in schema: {schema}")
    resources = {tbl: dlt.sources.sql_database.sql_table(engine, table=tbl, schema=schema) for tbl in table_names}
    return resources


def pipeline_creator_page():
    st.title("🚀 Create a Pipeline")
    st.markdown("Use this page to create, schedule and run your dlt pipeline. Fill out your Snowflake credentials, choose your source, and create the pipeline.")

    st.header("🔐 Snowflake Credentials")
    use_json = st.checkbox("Paste JSON credentials", value=False)
    if use_json:
        json_creds = st.text_area(
            "Paste Snowflake Credentials JSON", height=150,
            placeholder='{\n  "account": "YOUR_ACCOUNT",\n  "host": "YOUR_ACCOUNT",\n  "username": "YOUR_USERNAME",\n  "password": "YOUR_PASSWORD",\n  "role": "YOUR_ROLE",\n  "database": "YOUR_DATABASE",\n  "session_keep_alive": true\n}'
        )
        if st.button("Save Snowflake Credentials from JSON"):
            try:
                creds = json.loads(json_creds)
                st.session_state.snowflake_creds = creds
                st.success("Snowflake credentials saved from JSON!")
            except json.JSONDecodeError as e:
                st.error(f"Error parsing JSON: {e}")
    else:
        auth_type = st.selectbox("Authentication Type", options=["Key-Pair (JWT)", "Username/Password"])
        account = st.text_input("Account", placeholder="Your Snowflake account identifier.")
        username = st.text_input("Username", placeholder="Your Snowflake username.")
        if auth_type == "Key-Pair (JWT)":
            authenticator = "snowflake_jwt"
            private_key = st.text_area("Private Key (paste PEM formatted key)", height=150)
            password = ""  # Not used in JWT mode.
        else:
            authenticator = ""
            password = st.text_input("Password", placeholder="Your Snowflake password.", type="password")
            private_key = ""
        role = st.text_input("Role", placeholder="Your preferred Snowflake role.")
        database = st.text_input("Database", placeholder="Your Snowflake target database.")
        session_keep_alive = st.checkbox("Session Keep Alive", value=True)
        if st.button("Save Snowflake Credentials"):
            st.session_state.snowflake_creds = {
                "account": account,
                "username": username,
                "authenticator": authenticator,
                "private_key": private_key,
                "password": password,
                "role": role,
                "database": database,
                "schema": "",  # May be used by the dlt pipeline as needed.
                "host": account,
                "warehouse": "",
                "session_keep_alive": session_keep_alive
            }
            st.success("Snowflake credentials saved in session!")

    st.header("🚀 Pipeline Creator")
    config_mode = st.radio("Configuration Mode", options=["Manual Entry", "Select from Config"])

    source_config = None
    source_url = ""

    # ---------------------------
    # MANUAL ENTRY MODE
    # ---------------------------
    if config_mode == "Manual Entry":
        available_sources = get_verified_dlt_sources()
        selected_source = st.selectbox("📡 Select Data Source", available_sources)
        st.subheader(f"🔧 Configure {selected_source}")
        if selected_source == "REST API (Public)":
            source_url = st.text_input("📡 API Endpoint URL", "https://publicapi.example.com/data")
            st.subheader("Incremental Load Settings")
            incremental_type = st.selectbox("Load Type", options=["FULL", "INCREMENTAL"], index=0, key="api_public_incr")
            api_config = {"incremental_type": incremental_type}
            if incremental_type == "INCREMENTAL":
                primary_key = st.text_input("Primary Key Field", placeholder="e.g., id", key="api_public_pk")
                delta_column = st.text_input("Delta (Cursor) Field", placeholder="e.g., meta.updatedAt", key="api_public_delta_col")
                delta_value = st.text_input("Initial Delta Value", placeholder="e.g., 1900-01-01", key="api_public_delta_val")
                # For consistency, also set incremental.cursor_path equal to delta_column.
                api_config.update({"primary_key": primary_key, "delta_column": delta_column, "incremental": {"cursor_path": delta_column}, "delta_value": delta_value})
            source_config = {"endpoint_url": source_url}
            source_config.update(api_config)
        elif selected_source == "REST API (Private)":
            source_url = st.text_input("📡 API Endpoint URL", "https://privateapi.example.com/data")
            st.subheader("🔑 Authentication")
            st.radio("Auth Type", ["API Key", "OAuth", "Custom Headers"], key="api_private_auth")
            auth_config = st.text_area(
                "🔐 Enter Authentication Headers (JSON format)",
                """
{
    "Authorization": "Bearer YOUR_ACCESS_TOKEN",
    "x-api-key": "YOUR_API_KEY"
}
                """,
                height=150,
                key="api_private_auth_headers"
            )
            st.subheader("Incremental Load Settings")
            incremental_type = st.selectbox("Load Type", options=["FULL", "INCREMENTAL"], index=0, key="api_private_incr")
            api_config = {"incremental_type": incremental_type}
            if incremental_type == "INCREMENTAL":
                primary_key = st.text_input("Primary Key Field", placeholder="e.g., id", key="api_private_pk")
                delta_column = st.text_input("Delta (Cursor) Field", placeholder="e.g., meta.updatedAt", key="api_private_delta_col")
                delta_value = st.text_input("Initial Delta Value", placeholder="e.g., 1900-01-01", key="api_private_delta_val")
                api_config.update({"primary_key": primary_key, "delta_column": delta_column, "incremental": {"cursor_path": delta_column}, "delta_value": delta_value})
            try:
                parsed_auth_config = json.loads(auth_config)
            except json.JSONDecodeError:
                parsed_auth_config = {}
            source_config = {"endpoint_url": source_url, "auth": parsed_auth_config}
            source_config.update(api_config)
        elif selected_source == "oracle":
            source_config = {}
            source_config["host"] = st.text_input("Host", "localhost")
            source_config["port"] = st.number_input("Port", min_value=1, max_value=65535, value=1521)
            source_config["user"] = st.text_input("User", "admin")
            source_config["password"] = st.text_input("Password", type="password", key="oracle_db_password")
            database_input = st.text_input("Database / Service Name", "orcl")
            source_config["database"] = database_input
            source_config["service_name"] = database_input
            source_config["schema"] = st.text_input("Schema Name", "my_schema")
            source_config["db_type"] = "oracle"
            mode_val = st.radio("Mode", options=["Single Table", "Entire Database"], index=0)
            if mode_val == "Single Table":
                table = st.text_input("Source Table Name", "customers")
                source_config["mode"] = "sql_table"
                source_config["table"] = table.lower()
            else:
                source_config["mode"] = "sql_database"
            source_url = f"oracle://{source_config['host']}:{source_config['port']}/{source_config.get('service_name','')}"
        elif selected_source in ["postgres", "mysql", "bigquery", "redshift", "microsoft_sqlserver"]:
            source_config = {}
            source_config["host"] = st.text_input("Host", placeholder="The hostname of your server.")
            source_config["port"] = st.number_input("Port", min_value=1, max_value=65535, value=1433, placeholder="e.g., 1433")
            source_config["user"] = st.text_input("User", placeholder="Your SQL Server user.")
            source_config["password"] = st.text_input("Password", type="password", key="db_pass", placeholder="Your SQL Server password.")
            source_config["database"] = st.text_input("Database Name", placeholder="The SQL Server database name.")
            source_config["schema"] = st.text_input("Schema Name", placeholder="The SQL Server schema name (required).")
            source_config["db_type"] = selected_source
            mode_val = st.radio("Mode", options=["Single Table", "Entire Schema"], index=0)
            if mode_val == "Single Table":
                table = st.text_input("Source Table Name", placeholder="The SQL Server table you want to load.")
                source_config["mode"] = "sql_table"
                source_config["table"] = table
            else:
                source_config["mode"] = "sql_database"
            st.subheader("Incremental Load Settings")
            incremental_type = st.selectbox("Load Type", options=["FULL", "INCREMENTAL"], index=0)
            source_config["incremental_type"] = incremental_type
            if incremental_type == "INCREMENTAL":
                primary_key = st.text_input("Primary Key Column", placeholder="e.g., id")
                delta_column = st.text_input("Delta (Cursor) Column", placeholder="e.g., updated_at")
                delta_value = st.text_input("Initial Delta Value", placeholder="e.g., 1970-01-01T00:00:00Z")
                source_config["primary_key"] = primary_key
                source_config["delta_column"] = delta_column
                source_config["delta_value"] = delta_value

            source_url = f"{selected_source}://{source_config['host']}:{source_config['port']}/{source_config.get('database','')}"
        elif selected_source == "s3":
            source_config = {}
            source_config["bucket_name"] = st.text_input("S3 Bucket Name")
            source_config["access_key"] = st.text_input("Access Key")
            source_config["secret_key"] = st.text_input("Secret Key", type="password")
            source_url = ""
        elif selected_source in ["google_analytics", "google_ads", "google_search_console"]:
            source_config = {}
            source_config["account_id"] = st.text_input("Google Account ID")
            source_config["service_account_json"] = st.text_area("Service Account JSON", height=200)
            source_url = ""
        elif selected_source in ["zendesk", "hubspot", "pipedrive", "zoho_crm"]:
            source_config = {"api_key": st.text_input("API Key", type="password")}
            source_url = ""
        else:
            source_url = ""

    # ---------------------------
    # SELECT FROM CONFIG MODE
    # ---------------------------
    else:
        st.subheader("Select Configuration from Metadata")
        metadata_query = "SELECT * FROM config_metadata ORDER BY id"
        config_data = execute_query(metadata_query, fetch=True)
        if config_data:
            df = pd.DataFrame(
                config_data, 
                columns=[
                    "id", "source_type", "logical_name", "database_name", "schema_name", "table_name", 
                    "incremental_mode", 
                    "last_load_timestamp", "custom_query", "created_at", "updated_at", "delta_column", "delta_value", "primary_key"
                ]
            )
            df["source_type"] = df["source_type"].astype(str)
            df["logical_name"] = df["logical_name"].astype(str)
            df["database_name"] = df["database_name"].astype(str)
            df["schema_name"] = df["schema_name"].astype(str)
            
            available_source_types = sorted(df["source_type"].unique())
            selected_source_type = st.selectbox("Filter by Source Type", options=available_source_types, index=0)

            # Filter df by selected source type first
            df_source = df[df["source_type"] == selected_source_type]

            available_logical_names = sorted(df_source["logical_name"].unique())
            if available_logical_names:
                selected_logical_name = st.selectbox("Filter by Logical Name", options=available_logical_names, index=0)
            else:
                selected_logical_name = ""

            # Now filter by both source type and logical name
            filtered_df = df_source[df_source["logical_name"] == selected_logical_name]
            
            available_databases = sorted(filtered_df["database_name"].unique())
            if available_databases:
                # Automatically select the first available database (index 0)
                selected_database = st.selectbox("Select Database", options=available_databases, index=0)
            else:
                selected_database = ""
            df_db = filtered_df[filtered_df["database_name"] == selected_database]
            
            available_schemas = sorted(df_db["schema_name"].unique())
            if available_schemas:
                # Automatically select the first available schema (index 0)
                selected_schema = st.selectbox("Select Schema", options=available_schemas, index=0)
            else:
                selected_schema = ""
            df_schema = df_db[df_db["schema_name"] == selected_schema]
            
            available_tables = sorted(df_schema["table_name"].unique())
            load_all_tables = st.checkbox("Load ALL tables in selected schema", value=True)
            if load_all_tables:
                selected_tables = available_tables
            else:
                selected_tables = st.multiselect("Select Table(s)", options=available_tables)
            
            st.dataframe(df_schema)
            
            # For API configurations, dynamically construct config using metadata.
            if selected_source_type.lower() in ["api-public", "rest-api-public", "api-private", "rest api-private"]:
                # Use selected_database as domain and first selected table as endpoint path.
                endpoint = selected_tables[0] if selected_tables else ""
                common_config = {
                    "endpoint_url": f"https://{selected_database}/{endpoint}",
                    "data_selector": endpoint
                }
                # If metadata indicates incremental mode, update config accordingly.
                if not df_schema.empty:
                    row0 = df_schema.iloc[0]
                    if str(row0.get("incremental_mode", "")).upper() == "INCREMENTAL":
                        common_config.update({
                            "incremental_type": "INCREMENTAL",
                            "primary_key": row0.get("primary_key", ""),
                            "delta_column": row0.get("delta_column", ""),
                            "incremental": {"cursor_path": row0.get("delta_column", "")}
                        })
                    else:
                        common_config.update({"incremental_type": "FULL"})
                default_pipeline_name = f"{selected_source_type.replace(' ', '_')}-{selected_database}-{endpoint}"
            else:
                # For non-API sources, use existing behavior.
                common_config = {
                    "database": selected_database,
                    "schema": selected_schema,
                    "db_type": "microsoft_sqlserver" 
                                if selected_source_type.lower() in ["sql server", "microsoft sqlserver"] 
                                else selected_source_type.lower()
                }
                full_tables = []
                incr_tables = []
                for _, row in df_schema.iterrows():
                    tbl = row["table_name"]
                    if tbl in (selected_tables if not load_all_tables else available_tables):
                        if str(row["incremental_mode"]).upper() == "INCREMENTAL":
                            incr_tables.append(tbl)
                        else:
                            full_tables.append(tbl)
                
                configs_to_save = {}
                if full_tables:
                    full_config = common_config.copy()
                    full_config["mode"] = "sql_database"
                    full_config["tables"] = full_tables
                    full_config["incremental_type"] = "FULL"
                    configs_to_save["FULL"] = full_config
                if incr_tables:
                    incr_config = common_config.copy()
                    incr_config["mode"] = "sql_database"
                    incr_config["tables"] = incr_tables
                    incr_config["incremental_type"] = "INCREMENTAL"
                    row0 = df_schema[df_schema["table_name"].isin(incr_tables)].iloc[0]
                    incr_config["primary_key"] = row0.get("primary_key", "")
                    incr_config["delta_column"] = row0.get("delta_column", "")
                    incr_config["delta_value"] = row0.get("delta_value", "")
                    configs_to_save["INCR"] = incr_config

                host = st.text_input("Host for selected config", value="3.141.25.89")
                user = st.text_input("User for selected config", value="sa")
                pwd = st.text_input("Password for selected config", type="password")
                for key in configs_to_save:
                    configs_to_save[key]["host"] = host
                    configs_to_save[key]["user"] = user
                    configs_to_save[key]["password"] = pwd
                default_pipeline_name = f"{common_config.get('db_type','')}-{selected_database}-{selected_schema}-{'_'.join(selected_tables)}"
                # Merge FULL and INCR settings if available.
                common_config = {}
                if "FULL" in configs_to_save:
                    common_config.update(configs_to_save["FULL"])
                if "INCR" in configs_to_save:
                    common_config.update({
                        "incremental_type": configs_to_save["INCR"].get("incremental_type", "FULL"),
                        "primary_key": configs_to_save["INCR"].get("primary_key", ""),
                        "delta_column": configs_to_save["INCR"].get("delta_column", ""),
                        "delta_value": configs_to_save["INCR"].get("delta_value", "")
                    })
            default_pipeline_name = default_pipeline_name.replace(",", "_").replace(" ", "_")
            default_dataset_name = f"{selected_database.upper()}_{selected_schema.upper()}" if selected_source_type.lower() not in ["api-public", "rest-api-public", "api-private", "rest-api-private"] else selected_database.upper()
            source_config = common_config.copy()
            
            if selected_source_type.lower() in ["api-public", "rest api-public", "api-private", "rest-api-private"]:
                source_url = source_config.get("endpoint_url", "")
            else:
                source_url = f"microsoft_sqlserver://{host}:1433/{selected_database}"
        else:
            st.info("No configurations found in metadata.")

    # ---------------------------
    # Pipeline Details Section
    # ---------------------------
    st.subheader("🔄 Pipeline Details")
    name = st.text_input("Pipeline Name", placeholder="Give your pipeline a unique name.", 
                         value=default_pipeline_name if 'default_pipeline_name' in locals() else "")
    st.session_state.pipeline_name = name
    dataset_name = st.text_input("Snowflake Schema (Dataset)", placeholder="Name of your target schema in Snowflake.", 
                                 value=default_dataset_name if 'default_dataset_name' in locals() else "")
    target_table = st.text_input("Target Table Name", placeholder="Name of your target table in Snowflake.")
    
    schedule_option = st.radio("Run Mode", ("One-Time Run", "Schedule Recurring Run"))
    start_time_val = None
    if schedule_option == "Schedule Recurring Run":
        st.subheader("⏳ Schedule Configuration")
        start_date = st.date_input("Start Date", datetime.today().date())
        start_time_obj = st.time_input("Start Time", datetime.now().time())
        interval_unit = st.radio("Repeat Every:", ("Minutes", "Hours", "Days"), horizontal=True)
        if interval_unit == "Minutes":
            interval_minutes = st.slider("Every X Minutes", 1, 60, 5)
        elif interval_unit == "Hours":
            interval_minutes = st.slider("Every X Hours", 1, 24, 1) * 60
        elif interval_unit == "Days":
            interval_minutes = st.slider("Every X Days", 1, 30, 1) * 1440
        start_time_val = datetime.combine(start_date, start_time_obj).isoformat()
    
    st.code(json.dumps(source_config, default=str, indent=2), language="json")
    
    if st.button("🚀 Create Pipeline"):
        if config_mode == "Manual Entry":
            if selected_source in ["REST API (Private)", "REST API (Public)"]:
                config_key = name if name else selected_source
                save_source_config(config_key, source_config)
                # Insert a pipeline record for API sources:
                execute_query(
                    "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (get_next_pipeline_id(), config_key, source_url, target_table, dataset_name, 
                    "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time", "not run")
                )
                st.info(f"Configuration saved and pipeline record inserted for `{config_key}`!")
            elif selected_source in ["oracle", "postgres", "mysql", "bigquery", "redshift", "microsoft_sqlserver"]:
                config_key = name if name else selected_source
                save_source_config(config_key, source_config)
                # Insert a pipeline record for database sources:
                execute_query(
                    "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (get_next_pipeline_id(), config_key, source_url, target_table, dataset_name,
                    "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time", "not run")
                )
                st.info(f"Configuration saved and pipeline record inserted for `{config_key}`!")
        elif config_mode == "Select from Config" and source_config is not None:
            if selected_source_type.lower() not in ["api-public", "rest api-public", "api-private", "rest-api-private"]:
                if "FULL" in configs_to_save:
                    config_key = f"{name}-FULL"
                    save_source_config(config_key, configs_to_save["FULL"])
                    execute_query(
                        "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (get_next_pipeline_id(), f"{name}-FULL", source_url, target_table, dataset_name, 
                         "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time", "not run")
                    )
                    st.success(f"Pipeline `{name}-FULL` (Full load) created successfully!")
                if "INCR" in configs_to_save:
                    config_key = f"{name}-INCR"
                    save_source_config(config_key, configs_to_save["INCR"])
                    execute_query(
                        "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (get_next_pipeline_id(), f"{name}-INCR", source_url, target_table, dataset_name, 
                         "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time", "not run")
                    )
                    st.success(f"Pipeline `{name}-INCR` (Incremental load) created successfully!")
            else:
                # For API configs from metadata, simply save the constructed config.
                config_key = name
                save_source_config(config_key, source_config)
                execute_query(
                    "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (get_next_pipeline_id(), name, source_url, target_table, dataset_name, 
                     "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time", "not run")
                )
                st.success(f"Pipeline `{name}` created successfully!")
        st.success("Pipeline(s) created successfully!")
    
    st.subheader("📜 Existing Pipelines")
    pipelines = execute_query(
        "SELECT id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status FROM pipelines ORDER BY id DESC",
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
                            progress_placeholder = st.empty()
                            status_placeholder = st.empty()
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

                            for i in range(101):
                                if not pipeline_thread.is_alive():
                                    progress_placeholder.progress(100)
                                    break
                                progress_placeholder.progress(i)
                                status_placeholder.info(result_container["status"])
                                time.sleep(0.2)

                            status_placeholder.info(result_container["status"])
            st.markdown("---")
    else:
        st.info("No pipelines found. Create one above!")


if __name__ == "__main__":
    pipeline_creator_page()
