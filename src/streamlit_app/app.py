# app.py
import sys
import os
import streamlit as st

# Ensure the 'src' folder is in Python's module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Set Streamlit page title
st.set_page_config(page_title="EZMoveIt - Data Pipeline Manager", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Select a Page:",
    ["Pipeline Creator", "Monitoring Dashboard", "Pipeline Metrics", "Docs"],
)

# Load the correct page
if page == "Pipeline Creator":
    from src.streamlit_app.page_modules.pipeline_creator import pipeline_creator_page
    pipeline_creator_page()
elif page == "Monitoring Dashboard":
    from src.streamlit_app.page_modules.monitoring_dashboard import monitoring_dashboard_page
    monitoring_dashboard_page()
# elif page == "Execution Logs":
#     from src.streamlit_app.page_modules.execution_logs import execution_logs_page
    # execution_logs_page()
elif page == "Pipeline Metrics":
    from src.streamlit_app.page_modules.pipeline_metrics import pipeline_metrics_page
    pipeline_metrics_page()
# elif page == "Query Data":
#     from src.streamlit_app.page_modules.query_data import pipeline_query_page
#     pipeline_query_page()
elif page == "Docs":
    from src.streamlit_app.page_modules.docs import docs_page
    docs_page()