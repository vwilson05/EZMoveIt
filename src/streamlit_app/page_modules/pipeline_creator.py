import streamlit as st
import threading
import logging
import json
import os
from datetime import datetime, timedelta
from src.pipelines.dlt_pipeline import run_pipeline
from src.db.duckdb_connection import execute_query

CONFIG_DIR = "config"
os.makedirs(CONFIG_DIR, exist_ok=True)  # Ensure config directory exists

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ✅ Hardcoded List of Verified Sources + REST API Options
def get_verified_dlt_sources():
    return [
        "REST API (Public)", "REST API (Private)",  # ✅ Added REST API
        "airtable", "asana", "bigquery", "braintree", "chargebee", "copper",
        "dynamodb", "facebook_ads", "freshdesk", "github", "google_analytics",
        "google_ads", "google_search_console", "greenhouse", "hubspot",
        "intercom", "jira", "klaviyo", "lever", "linkedin_ads", "microsoft_ads",
        "microsoft_sqlserver", "mixpanel", "monday", "mysql", "notion",
        "pagerduty", "pendo", "pipedrive", "postgres", "quickbooks", "recharge",
        "redshift", "s3", "salesforce", "sendgrid", "shopify", "slack",
        "snapchat_ads", "snowflake", "square", "stripe", "tempo", "todoist",
        "twilio", "twitter", "woocomerce", "xero", "yandex_metrica",
        "zendesk", "zoho_crm"
    ]

# ✅ Function to Get the Next Available ID for `pipelines`
def get_next_pipeline_id():
    """Fetches the next available pipeline ID."""
    result = execute_query("SELECT MAX(id) FROM pipelines", fetch=True)
    next_id = (result[0][0] + 1) if result and result[0][0] is not None else 1
    return next_id

# Function to save source configuration
def save_source_config(source_name, config_data):
    """Saves connection details to a config file."""
    config_path = os.path.join(CONFIG_DIR, f"{source_name.replace(' ', '_').lower()}_config.json")
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=4)

# --- Streamlit UI ---
def pipeline_creator_page():
    st.title("🚀 Pipeline Creator")

    # ✅ Use Hardcoded Verified Sources
    available_sources = get_verified_dlt_sources()
    selected_source = st.selectbox("📡 Select Data Source", available_sources)

    # 🔹 Step 2: Enter Connection Details (Dynamic Fields)
    source_config = {}
    st.subheader(f"🔧 Configure {selected_source}")

    # ✅ REST API (Public) - Only needs the API URL
    if selected_source == "REST API (Public)":
        source_url = st.text_input("📡 API Endpoint URL", "https://api.example.com/data")

    # ✅ REST API (Private) - Needs API URL + Authentication
    elif selected_source == "REST API (Private)":
        source_url = st.text_input("📡 API Endpoint URL", "https://api.example.com/data")
        st.subheader("🔑 Authentication")
        auth_type = st.radio("Auth Type", ["API Key", "OAuth", "Custom Headers"])

        # Free text input for key-value pairs (JSON format)
        auth_config = st.text_area("🔐 Enter Authentication Headers (JSON format)", """
{
    "Authorization": "Bearer YOUR_ACCESS_TOKEN",
    "x-api-key": "YOUR_API_KEY"
}
        """, height=150)

        # Validate JSON input
        try:
            parsed_auth_config = json.loads(auth_config)
            source_config["auth"] = parsed_auth_config
        except json.JSONDecodeError:
            st.error("⚠️ Invalid JSON format in Authentication Headers")

    # ✅ Other Verified Sources - Standard config fields
    elif selected_source in ["postgres", "mysql", "bigquery", "redshift", "microsoft_sqlserver"]:
        source_config["host"] = st.text_input("Host", "localhost")
        source_config["port"] = st.number_input("Port", min_value=1, max_value=65535, value=5432)
        source_config["user"] = st.text_input("User", "admin")
        source_config["password"] = st.text_input("Password", type="password")
        source_config["database"] = st.text_input("Database Name", "my_database")

    elif selected_source == "s3":
        source_config["bucket_name"] = st.text_input("S3 Bucket Name")
        source_config["access_key"] = st.text_input("Access Key")
        source_config["secret_key"] = st.text_input("Secret Key", type="password")

    elif selected_source in ["google_analytics", "google_ads", "google_search_console"]:
        source_config["account_id"] = st.text_input("Google Account ID")
        source_config["service_account_json"] = st.text_area("Service Account JSON", height=200)

    elif selected_source in ["zendesk", "hubspot", "pipedrive", "zoho_crm"]:
        source_config["api_key"] = st.text_input("API Key", type="password")

    # 🔹 Step 3: Save Configuration (for all sources except Public API)
    if selected_source != "REST API (Public)" and st.button("💾 Save Source Configuration"):
        save_source_config(selected_source, source_config)
        st.success(f"✅ Configuration saved for `{selected_source}`!")

    # 🔹 Step 4: Pipeline Creation Details
    st.subheader("🔄 Pipeline Details")
    name = st.text_input("Pipeline Name", "new_pipeline")
    target_table = st.text_input("Target Table Name", "destination_table")
    dataset_name = st.text_input("Snowflake Schema (Dataset)", "DATA_SCHEMA")

    # 🔹 Option for Scheduling vs. One-Time Run
    schedule_option = st.radio("Run Mode", ("One-Time Run", "Schedule Recurring Run"))

    start_time = None
    interval_minutes = None

    if schedule_option == "Schedule Recurring Run":
        st.subheader("⏳ Schedule Configuration")

        # Select start date and time
        start_date = st.date_input("Start Date", datetime.today().date())
        start_time = st.time_input("Start Time", datetime.now().time())

        # Frequency Selection
        interval_unit = st.radio("Repeat Every:", ("Minutes", "Hours", "Days"), horizontal=True)

        # Select interval value
        if interval_unit == "Minutes":
            interval_minutes = st.slider("Every X Minutes", 1, 60, 5)
        elif interval_unit == "Hours":
            interval_minutes = st.slider("Every X Hours", 1, 24, 1) * 60
        elif interval_unit == "Days":
            interval_minutes = st.slider("Every X Days", 1, 30, 1) * 1440  # Convert to minutes

        # Convert start date & time into full timestamp
        start_time = datetime.combine(start_date, start_time).isoformat()

    # 🔹 Submit Button
    if st.button("🚀 Create Pipeline"):
        schedule_type = "Scheduled" if schedule_option == "Schedule Recurring Run" else "One-Time"
        
        # ✅ Get the next pipeline ID manually
        pipeline_id = get_next_pipeline_id()
        
        execute_query(
            "INSERT INTO pipelines (id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (pipeline_id, name, source_url, target_table, dataset_name, schedule_type, "not run")
        )
        st.success(f"✅ Pipeline `{name}` created successfully!")
        
    # --- **📜 Show Existing Pipelines** ---
    st.header("📜 Existing Pipelines")
    
    pipelines = execute_query("SELECT id, name, source_url, snowflake_target, dataset_name, schedule, last_run_status FROM pipelines ORDER BY id DESC", fetch=True)

    if pipelines:
        for row in pipelines:
            pid, name, src, tgt, dataset, schedule, status = row

            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**🆔 {pid} | 📌 Name:** `{name}`")
                st.markdown(f"🔗 **Source URL:** `{src}`")
                st.markdown(f"🎯 **Target Table:** `{tgt}` | 🏛 **Schema:** `{dataset}`")
                st.markdown(f"⏰ **Schedule:** `{schedule if schedule else 'One-Time Run'}`")
                st.markdown(f"📌 **Last Run Status:** `{status}`")

            with col2:
                if status == "running":
                    st.button("⏳ Running...", key=f"disabled_{pid}", disabled=True)
                else:
                    if st.button(f"▶️ Run {name}", key=f"trigger_{pid}"):
                        threading.Thread(target=run_pipeline, args=(name, dataset, tgt), daemon=True).start()
            
            st.markdown("---")
    
    else:
        st.info("🚀 No pipelines found. Create one above!")
