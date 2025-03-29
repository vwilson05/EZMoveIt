import streamlit as st
import json
import os
from src.db.duckdb_connection import execute_query

CONFIG_DIR = "config"
os.makedirs(CONFIG_DIR, exist_ok=True)

def save_source_config(source_key, config_data):
    config_path = os.path.join(CONFIG_DIR, f"{source_key.replace(' ', '_').lower()}_config.json")
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=2, default=str)

def get_pipeline_names():
    """Get all pipeline names for the dropdown."""
    query = "SELECT id, name FROM pipelines ORDER BY name"
    results = execute_query(query, fetch=True)
    return results

def get_pipeline_details(pipeline_id):
    """Get pipeline details by ID."""
    query = """
    SELECT name, dataset_name, target_table, source_url, source_config
    FROM pipelines
    WHERE id = ?
    """
    result = execute_query(query, (pipeline_id,), fetch=True)
    if result:
        return {
            'name': result[0][0],
            'dataset_name': result[0][1],
            'target_table': result[0][2],
            'source_url': result[0][3],
            'source_config': result[0][4]
        }
    return None

def pipeline_editor_page():
    st.title("✏️ Edit Pipeline")
    
    # Get all pipelines for the dropdown
    pipelines = get_pipeline_names()
    if not pipelines:
        st.error("No pipelines found. Please create a pipeline first.")
        if st.button("Go to Pipeline Creator"):
            st.session_state.current_page = "Pipeline Creator"
            st.rerun()
        return
    
    # Create pipeline selection dropdown
    pipeline_options = {f"{p[1]} (ID: {p[0]})": p[0] for p in pipelines}
    selected_pipeline = st.selectbox(
        "Select Pipeline to Edit",
        options=list(pipeline_options.keys()),
        index=0
    )
    
    # Get pipeline details
    pipeline_id = pipeline_options[selected_pipeline]
    pipeline_details = get_pipeline_details(pipeline_id)
    
    if not pipeline_details:
        st.error("Could not load pipeline details.")
        return
    
    st.info(f"Editing pipeline: {pipeline_details['name']}")
    
    # Pipeline details
    name = st.text_input("Pipeline Name", value=pipeline_details['name'])
    dataset_name = st.text_input("Snowflake Schema (Dataset)", value=pipeline_details['dataset_name'])
    target_table = st.text_input("Target Table Name", value=pipeline_details['target_table'])
    
    # Source configuration
    st.subheader("🔧 Source Configuration")
    source_url = st.text_input("Source URL", value=pipeline_details['source_url'])
    
    # Load existing source config
    source_config = json.loads(pipeline_details['source_config']) if pipeline_details['source_config'] else {}
    
    # Authentication settings
    if source_config.get('auth_type') == 'bearer':
        st.subheader("🔑 Authentication")
        # Extract token from auth.Authorization if it exists
        current_token = ""
        if 'auth' in source_config and 'Authorization' in source_config['auth']:
            current_token = source_config['auth']['Authorization'].replace('Bearer ', '')
        bearer_token = st.text_input("Bearer Token", type="password", value="********" if current_token else "")
        if bearer_token != "********":
            source_config['auth'] = {
                "Authorization": f"Bearer {bearer_token}"
            }
    
    # Action buttons
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("💾 Save Changes"):
            try:
                # Update pipeline record
                execute_query(
                    """
                    UPDATE pipelines 
                    SET name = ?, 
                        dataset_name = ?, 
                        target_table = ?,
                        source_url = ?,
                        source_config = ?
                    WHERE id = ?
                    """,
                    (name, dataset_name, target_table, source_url, json.dumps(source_config), pipeline_id)
                )
                
                # Update source config JSON file
                config_data = {
                    "endpoint_url": source_url,
                    "auth": source_config.get("auth", {}),
                    "incremental_type": "FULL"
                }
                save_source_config(name, config_data)
                
                st.success("Pipeline updated successfully!")
                st.rerun()  # Refresh the page to show updated data
            except Exception as e:
                st.error(f"Error updating pipeline: {str(e)}")
    
    with col2:
        if st.button("❌ Cancel Edit"):
            # Switch back to Pipeline Runs
            st.session_state.current_page = "Pipeline Runs"
            st.rerun() 