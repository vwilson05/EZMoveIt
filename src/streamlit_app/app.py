# app.py
import sys
import os
import streamlit as st

# Ensure the 'src' folder is in Python's module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Set Streamlit page title
st.set_page_config(page_title="EZMoveIt - Data Pipeline Manager", layout="wide")

# ---- NEW: Credentials Input Form in Sidebar ----
with st.sidebar.expander("Snowflake Credentials", expanded=False):
    with st.form(key="snowflake_creds_form"):
        account = st.text_input("Account")
        username = st.text_input("Username")
        authenticator = st.text_input("Authenticator", value="snowflake_jwt")
        private_key = st.text_area("Private Key (paste PEM formatted key)", height=150)
        role = st.text_input("Role", value="SYSADMIN")
        database = st.text_input("Database")
        schema = st.text_input("Schema")
        host = st.text_input("Host")
        session_keep_alive = st.checkbox("Session Keep Alive", value=True)
        
        submit_creds = st.form_submit_button(label="Save Credentials")
        
        if submit_creds:
            st.session_state.snowflake_creds = {
                "account": account,
                "username": username,
                "authenticator": authenticator,
                "private_key": private_key,
                "role": role,
                "database": database,
                "schema": schema,
                "host": host,
                "session_keep_alive": session_keep_alive
            }
            st.success("Snowflake credentials saved in session!")

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a Page:",
    ["Pipeline Creator", "Execution Logs", "Pipeline Metrics", "Query Data"],
)

# Load the correct page
if page == "Pipeline Creator":
    from src.streamlit_app.page_modules.pipeline_creator import pipeline_creator_page
    pipeline_creator_page()
elif page == "Execution Logs":
    from src.streamlit_app.page_modules.execution_logs import execution_logs_page
    execution_logs_page()
elif page == "Pipeline Metrics":
    from src.streamlit_app.page_modules.pipeline_metrics import pipeline_metrics_page
    pipeline_metrics_page()
elif page == "Query Data":
    from src.streamlit_app.page_modules.query_data import pipeline_query_page
    pipeline_query_page()
