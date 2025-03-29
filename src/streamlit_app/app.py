# app.py
import sys
import os
import streamlit as st
from streamlit_option_menu import option_menu

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_root)

# Import page modules after path is set
from src.streamlit_app.page_modules.pipeline_creator import pipeline_creator_page
from src.streamlit_app.page_modules.pipeline_editor import pipeline_editor_page
from src.streamlit_app.page_modules.pipeline_runs import pipeline_runs_page
from src.streamlit_app.page_modules.monitoring_dashboard import monitoring_dashboard
from src.streamlit_app.page_modules.settings import settings_page
from src.db.duckdb_connection import reinitialize_database

# Set Streamlit page title
st.set_page_config(
    page_title="EZMoveIt - ETL Pipeline Manager",
    page_icon="🚀",
    layout="wide"
)

# Initialize database if it doesn't exist
if not os.path.exists(os.path.join(project_root, "data", "EZMoveIt.duckdb")):
    reinitialize_database()

# Initialize session state for page navigation if not exists
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Pipeline Creator"

# Sidebar navigation
with st.sidebar:
    selected = option_menu(
        None,
        ["Pipeline Creator", "Pipeline Runs", "Pipeline Editor", "Monitoring", "Settings"],
        icons=['house', 'play-circle', 'gear', "list-task", 'cog'],
        menu_icon="cast",
        default_index=["Pipeline Creator", "Pipeline Runs", "Pipeline Editor", "Monitoring", "Settings"].index(st.session_state.current_page)
    )

# Update session state if page selection changes
if selected != st.session_state.current_page:
    st.session_state.current_page = selected
    st.rerun()

# Main content area
if st.session_state.current_page == "Pipeline Creator":
    pipeline_creator_page()
elif st.session_state.current_page == "Pipeline Runs":
    pipeline_runs_page()
elif st.session_state.current_page == "Pipeline Editor":
    pipeline_editor_page()
elif st.session_state.current_page == "Monitoring":
    monitoring_dashboard()
elif st.session_state.current_page == "Settings":
    settings_page()