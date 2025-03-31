# app.py
import sys
import os
import streamlit as st
from streamlit_option_menu import option_menu
# from styles.theme_manager import load_theme

# Set Streamlit page title
st.set_page_config(
    page_title="EZMoveIt - ETL Pipeline Manager",
    page_icon="🚀",
    layout="wide"
)

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_root)

# Import page modules after path is set
from src.streamlit_app.page_modules.pipeline_creator import pipeline_creator_page
from src.streamlit_app.page_modules.pipeline_list import pipeline_list_page
from src.streamlit_app.page_modules.pipeline_runs import pipeline_runs_page
from src.streamlit_app.page_modules.pipeline_editor import pipeline_editor_page
from src.streamlit_app.page_modules.monitoring_dashboard import monitoring_dashboard
from src.streamlit_app.page_modules.settings import settings_page
from src.streamlit_app.page_modules.metadata_config import metadata_config_page
from src.streamlit_app.page_modules.docs import docs_page
from src.db.duckdb_connection import reinitialize_database


# # Load theme before any other UI elements
# load_theme()

# Initialize database if it doesn't exist
if not os.path.exists(os.path.join(project_root, "data", "EZMoveIt.duckdb")):
    reinitialize_database()

from streamlit_option_menu import option_menu
import streamlit as st

# Split visually with section titles or dividers
menu_options = [
    "Create Pipeline", "Pipelines", "Pipeline Runs", "Edit Pipeline", "Monitoring",
    "---",
    "Settings", "Metadata Config", "Docs"
]

icons = [
    'house', 'list-task', 'play-circle', 'gear', 'sliders',
    None,
    'database', 'table', 'book'
]

with st.sidebar:
    selected = option_menu(
        menu_title=None,
        options=menu_options,
        icons=icons,
        menu_icon="cast",
        default_index=0
    )

# At the top of your app.py, after other imports
def apply_custom_style():
    st.markdown("""
        <style>
        /* Navigation styling for light mode */
        [data-testid="stSidebarNav"] {
            background-color: #f8f9fa !important;
            border-right: 1px solid #0d6efd !important;
        }
        
        [data-testid="stSidebarNav"] button {
            background-color: #f8f9fa !important;
            border-color: #0d6efd !important;
            color: black !important;
        }
        
        [data-testid="stSidebarNav"] button:hover {
            background-color: #e9ecef !important;
            border-color: #0a58ca !important;
        }
        
        /* Hide yellow warning bar globally */
        .stWarning {
            display: none !important;
        }
        </style>
    """, unsafe_allow_html=True)

def main():
    # Add this line after st.set_page_config
    if st.session_state.get('theme') == 'light':
        apply_custom_style()
    # Main content area
    if selected == "Create Pipeline":
        pipeline_creator_page()
    elif selected == "Pipelines":
        pipeline_list_page()
    elif selected == "Pipeline Runs":
        pipeline_runs_page()
    elif selected == "Edit Pipeline":
        pipeline_editor_page()
    elif selected == "Monitoring":
        monitoring_dashboard()
    elif selected == "Settings":
        settings_page()
    elif selected == "Metadata Config":
        metadata_config_page()
    elif selected == "Docs":
        docs_page()

if __name__ == "__main__":
    main()
