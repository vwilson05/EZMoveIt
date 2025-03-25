# pipeline_creator.py
import streamlit as st
import threading
import logging
import json
import os
import time
from datetime import datetime
from src.pipelines.dlt_pipeline import run_pipeline_with_creds
from src.db.duckdb_connection import execute_query

CONFIG_DIR = "config"
os.makedirs(CONFIG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_verified_dlt_sources():
    return [
        "REST API (Public)",
        "REST API (Private)",
        "airtable",
        "asana",
        "bigquery",
        "braintree",
        "chargebee",
        "copper",
        "dynamodb",
        "facebook_ads",
        "freshdesk",
        "github",
        "google_analytics",
        "google_ads",
        "google_search_console",
        "greenhouse",
        "hubspot",
        "intercom",
        "jira",
        "klaviyo",
        "lever",
        "linkedin_ads",
        "microsoft_ads",
        "microsoft_sqlserver",
        "mixpanel",
        "monday",
        "mysql",
        "notion",
        "pagerduty",
        "pendo",
        "pipedrive",
        "postgres",
        "quickbooks",
        "recharge",
        "redshift",
        "s3",
        "salesforce",
        "sendgrid",
        "shopify",
        "slack",
        "snapchat_ads",
        "snowflake",
        "square",
        "stripe",
        "tempo",
        "todoist",
        "twilio",
        "twitter",
        "woocomerce",
        "xero",
        "yandex_metrica",
        "zendesk",
        "zoho_crm",
    ]

def get_next_pipeline_id():
    result = execute_query("SELECT MAX(id) FROM pipelines", fetch=True)
    next_id = (result[0][0] + 1) if result and result[0][0] is not None else 1
    return next_id

def save_source_config(source_key, config_data):
    config_path = os.path.join(CONFIG_DIR, f"{source_key.replace(' ', '_').lower()}_config.json")
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=4)

def pipeline_creator_page():
    st.title("🚀 Create a Pipeline")
    st.markdown("Use this page to create, schedule and run your dlt pipeline. Fill out your Snowflake credentials, choose your source, and create the pipeline.")
    
    st.header("🔐 Snowflake Credentials")
    auth_type = st.selectbox("Authentication Type", options=["Key-Pair (JWT)", "Username/Password"])
    account = st.text_input("Account")
    username = st.text_input("Username")
    if auth_type == "Key-Pair (JWT)":
        authenticator = "snowflake_jwt"
        private_key = st.text_area("Private Key (paste PEM formatted key)", height=150)
        password = ""  # Not used in JWT mode.
    else:
        authenticator = ""
        password = st.text_input("Password", type="password")
        private_key = ""
    role = st.text_input("Role", value="SYSADMIN")
    database = st.text_input("Database")
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
            "schema": "",
            "host": account,
            "warehouse": "",
            "session_keep_alive": session_keep_alive
        }
        st.success("Snowflake credentials saved in session!")
    
    st.header("🚀 Pipeline Creator")
    # Source configuration.
    available_sources = get_verified_dlt_sources()
    selected_source = st.selectbox("📡 Select Data Source", available_sources)

    st.subheader(f"🔧 Configure {selected_source}")
    if selected_source == "REST API (Public)":
        source_url = st.text_input("📡 API Endpoint URL", "https://publicapi.example.com/data")
    elif selected_source == "REST API (Private)":
        source_url = st.text_input("📡 API Endpoint URL", "https://privateapi.example.com/data")
        st.subheader("🔑 Authentication")
        st.radio("Auth Type", ["API Key", "OAuth", "Custom Headers"])
        auth_config = st.text_area(
            "🔐 Enter Authentication Headers (JSON format)",
            """
{
    "Authorization": "Bearer YOUR_ACCESS_TOKEN",
    "x-api-key": "YOUR_API_KEY"
}
            """,
            height=150,
        )
    elif selected_source in ["postgres", "mysql", "bigquery", "redshift", "microsoft_sqlserver"]:
        source_config = {}
        source_config["host"] = st.text_input("Host", "localhost")
        source_config["port"] = st.number_input("Port", min_value=1, max_value=65535, value=5432)
        source_config["user"] = st.text_input("User", "admin")
        source_config["password"] = st.text_input("Password", type="password")
        source_config["database"] = st.text_input("Database Name", "my_database")
        source_url = ""
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

    st.subheader("🔄 Pipeline Details")
    name = st.text_input("Pipeline Name", "", placeholder="Give your pipeline a unique name.")
    # Save pipeline name in session to be reused in config if needed.
    st.session_state.pipeline_name = name
    dataset_name = st.text_input("Snowflake Schema (Dataset)", "", placeholder="Name of your target schema in Snowflake.")
    target_table = st.text_input("Target Table Name", "", placeholder="Name of your target table in Snowflake.")

    schedule_option = st.radio("Run Mode", ("One-Time Run", "Schedule Recurring Run"))
    start_time = None
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
        start_time = datetime.combine(start_date, start_time_obj).isoformat()

    if st.button("🚀 Create Pipeline"):
        # For REST API (Private), automatically save the auth configuration
        if selected_source == "REST API (Private)":
            try:
                parsed_auth_config = json.loads(auth_config)
                source_config = {"auth": parsed_auth_config}
                # Use the pipeline name as the key; if missing, fall back to the source label
                config_key = name if name else selected_source
                save_source_config(config_key, source_config)
                st.info(f"Configuration saved for `{config_key}`!")
            except json.JSONDecodeError:
                st.error("Invalid JSON format in Authentication Headers")
                st.stop()  # Stop pipeline creation if the config is invalid

        schedule_type = "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time"
        pipeline_id = get_next_pipeline_id()
        execute_query(
            "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (pipeline_id, name, source_url, target_table, dataset_name, schedule_type, "not run"),
        )
        st.success(f"Pipeline `{name}` created successfully!")

    st.subheader("📜 Existing Pipelines")
    pipelines = execute_query(
        "SELECT id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status FROM pipelines ORDER BY id DESC",
        fetch=True,
    )
    if pipelines:
        for row in pipelines:
            pid, name, src, tgt, dataset, schedule, status = row
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**ID:** `{pid}` | **Name:** `{name}`")
                st.markdown(f"**Source URL:** `{src}`")
                st.markdown(f"**Target Table:** `{tgt}` | **Schema:** `{dataset}`")
                st.markdown(f"**Schedule:** `{schedule if schedule else 'One-Time Run'}`")
                st.markdown(f"**Last Run Status:** `{status}`")
            with col2:
                if status == "running":
                    st.button("Running...", key=f"disabled_{pid}", disabled=True)
                else:
                    if st.button(f"Run {name}", key=f"trigger_{pid}"):
                        creds = st.session_state.get("snowflake_creds")
                        if not creds:
                            st.error("No Snowflake credentials found. Please enter them above.")
                        else:
                            progress_placeholder = st.empty()
                            status_placeholder = st.empty()
                            result_container = {"status": "Pipeline started...", "result": None}
                            
                            def run_pipeline_thread():
                                result_container["status"] = "Pipeline started..."
                                res = run_pipeline_with_creds(name, dataset, tgt, creds)
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
