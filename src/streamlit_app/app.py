# app.py
import sys
import os
import streamlit as st
from streamlit_option_menu import option_menu

# Set Streamlit page title
st.set_page_config(page_title="EZMoveIt - Data Pipeline Manager", layout="wide")


# Ensure the 'src' folder is in Python's module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Sidebar navigation
with st.sidebar:
    selected = option_menu(None,
        ["Pipeline Creator", "Monitoring Dashboard", "Pipeline Metrics", "Docs"], 
        icons=['house', 'gear', "list-task", 'book'], 
        menu_icon="cast", default_index=0,
    )

# Load the correct page
if selected == "Pipeline Creator":
    from src.streamlit_app.page_modules.pipeline_creator import pipeline_creator_page
    pipeline_creator_page()
elif selected == "Monitoring Dashboard":
    from src.streamlit_app.page_modules.monitoring_dashboard import monitoring_dashboard_page
    monitoring_dashboard_page()
# elif page == "Execution Logs":
#     from src.streamlit_app.page_modules.execution_logs import execution_logs_page
    # execution_logs_page()
elif selected == "Pipeline Metrics":
    from src.streamlit_app.page_modules.pipeline_metrics import pipeline_metrics_page
    pipeline_metrics_page()
# elif page == "Query Data":
#     from src.streamlit_app.page_modules.query_data import pipeline_query_page
#     pipeline_query_page()
elif selected == "Docs":
    from src.streamlit_app.page_modules.docs import docs_page
    docs_page()