import streamlit as st
import pandas as pd
import logging
import json
import os
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def get_config_path(filename):
    """Returns the correct config file path based on environment (Docker vs. local)."""
    running_in_docker = os.getenv("RUNNING_IN_DOCKER", "").strip().lower()
    local_path = os.path.abspath(os.path.join(os.getcwd(), "config", filename))
    docker_path = f"/app/config/{filename}"
    return docker_path if running_in_docker == "true" else local_path

def load_snowflake_credentials():
    """
    Loads Snowflake credentials with priority:
      1. st.session_state
      2. st.secrets (ideal for Streamlit Cloud)
      3. Config file
    """
    if "snowflake_creds" in st.session_state:
        logging.info("Using Snowflake credentials from session state.")
        return st.session_state.snowflake_creds
    elif hasattr(st, "secrets") and st.secrets.get("snowflake"):
        logging.info("Using Snowflake credentials from st.secrets.")
        return st.secrets["snowflake"]
    else:
        config_path = get_config_path("snowflake_config.json")
        if not os.path.exists(config_path):
            logging.error(f"❌ Snowflake config file missing! Expected at: {config_path}")
            return {}
        with open(config_path, "r") as f:
            creds = json.load(f)
        if creds.get("authenticator") == "snowflake_jwt":
            original_key_path = creds.get("private_key_path", "/app/config/rsa_key.p8")
            corrected_key_path = get_config_path(os.path.basename(original_key_path))
            creds["private_key_path"] = corrected_key_path
            if not os.path.exists(corrected_key_path):
                logging.error(f"❌ Private key file missing at {corrected_key_path}!")
                return {}
            try:
                with open(corrected_key_path, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None,
                        backend=default_backend(),
                    )
                    creds["private_key"] = private_key.private_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption(),
                    ).decode("utf-8")
            except Exception as e:
                logging.error(f"❌ Failed to load private key: {e}")
                return {}
        return creds

# Load credentials (session_state or st.secrets will be used on Streamlit Cloud)
creds = load_snowflake_credentials()
if not creds or (creds.get("authenticator") == "snowflake_jwt" and "private_key" not in creds):
    st.error("❌ Failed to load Snowflake credentials. Check logs for details.")
    # Note: We avoid st.stop() so the error message is rendered

def get_snowflake_connection():
    try:
        if creds.get("authenticator") == "snowflake_jwt":
            return connect(
                user=creds["username"],
                account=creds["account"],
                role=creds["role"],
                database=creds["database"],
                schema=creds["schema"],
                private_key=serialization.load_pem_private_key(
                    creds["private_key"].encode("utf-8"),
                    password=None,
                    backend=default_backend(),
                )
            )
        else:
            return connect(
                user=creds["username"],
                password=creds["password"],
                account=creds["account"],
                role=creds["role"],
                database=creds["database"],
                schema=creds["schema"]
            )
    except Exception as e:
        logging.error(f"❌ Snowflake connection failed: {e}")
        st.error("🚨 Could not connect to Snowflake. Check logs for details.")
        return None

def query_data(query):
    conn = get_snowflake_connection()
    if conn is None:
        return None
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(data, columns=columns)

def pipeline_query_page():
    st.title("📊 Data Explorer - Snowflake")
    query = st.text_area("📝 Enter SQL Query", "SELECT * FROM DLT_TEST.TODO LIMIT 10")
    if st.button("Run Query"):
        df = query_data(query)
        if df is not None:
            st.dataframe(df)
        else:
            st.error("Query failed.")

if __name__ == "__main__":
    pipeline_query_page()
