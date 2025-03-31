import streamlit as st
import pandas as pd
import json
from src.db.duckdb_connection import execute_query

def metadata_config_page():
    st.title("Metadata Configuration")
    st.caption("Manage metadata configurations for pipeline sources")
    
    # Display a placeholder message
    st.info("This feature is under development. You will be able to create and manage metadata configurations here.")
    
    # Display existing metadata configs (if any)
    query = """
    SELECT name, source_type, source_url, config
    FROM source_configs
    ORDER BY name
    """
    results = execute_query(query, fetch=True)
    
    if results:
        st.subheader("Existing Metadata Configurations")
        
        # Convert to dataframe for display
        df = pd.DataFrame(results, columns=["Name", "Source Type", "Source URL", "Config"])
        
        # Display the dataframe
        st.dataframe(df.drop(columns=["Config"]))
    else:
        st.write("No metadata configurations found.") 