import streamlit as st
import pandas as pd
import json
import threading
from datetime import datetime
from src.db.duckdb_connection import execute_query
from src.pipelines.dlt_pipeline import run_pipeline_with_creds
import logging

def update_job_status(job_id, status, message=""):
    """Update the status of a pipeline job in session state"""
    status_key = f"pipeline_{job_id}_status"
    st.session_state[status_key] = f"{status.capitalize()}: {message}" if message else status.capitalize()

def run_pipeline_thread(pipeline_name, dataset_name, target_table, job_id, creds):
    try:
        # Update status to running
        update_job_status(job_id, "running", "Pipeline started")
        
        # Run the pipeline with passed credentials
        run_pipeline_with_creds(pipeline_name, dataset_name, target_table, creds)
        
        # Update status to success
        update_job_status(job_id, "success", "Pipeline completed successfully")
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Pipeline failed: {error_msg}")
        update_job_status(job_id, "failed", error_msg)

def pipeline_list_page():
    st.title("Pipeline Management")
    st.caption("View and manage all created pipelines")
    
    # Fetch all pipelines from the database
    query = """
    SELECT 
        id, 
        name, 
        source_url, 
        target_table, 
        dataset_name, 
        schedule,
        created_at,
        metadata_selection
    FROM pipelines
    ORDER BY created_at DESC
    """
    pipelines = execute_query(query, fetch=True)
    
    if not pipelines:
        st.info("No pipelines found. Create a pipeline first.")
        return
    
    # Group pipelines by schedule type
    scheduled_pipelines = []
    ad_hoc_pipelines = []
    
    for pipeline in pipelines:
        pipeline_id, name, source_url, target_table, dataset_name, schedule_json, created_at, metadata_selection_json = pipeline
        
        # Parse the schedule if it exists
        schedule = None
        if schedule_json:
            try:
                schedule = json.loads(schedule_json)
            except:
                schedule = None
        
        # Parse metadata selection if it exists
        is_metadata_driven = False
        metadata_selection = None
        if metadata_selection_json:
            try:
                metadata_selection = json.loads(metadata_selection_json)
                is_metadata_driven = True
            except:
                metadata_selection = None
        
        pipeline_data = {
            "id": pipeline_id,
            "name": name,
            "source_url": source_url,
            "target_table": target_table,
            "dataset_name": dataset_name,
            "created_at": created_at,
            "schedule": schedule,
            "is_metadata_driven": is_metadata_driven,
            "metadata_selection": metadata_selection
        }
        
        if schedule and schedule.get('type', '').lower() != 'manual':
            scheduled_pipelines.append(pipeline_data)
        else:
            ad_hoc_pipelines.append(pipeline_data)
    
    # Add legend for the icons
    st.markdown("""
    <style>
    .metadata-icon {
        color: #2E86C1;
        font-size: 20px;
    }
    .manual-icon {
        color: #808080;
        font-size: 20px;
    }
    .legend-container {
        display: flex;
        align-items: center;
        margin-bottom: 15px;
    }
    .legend-item {
        display: flex;
        align-items: center;
        margin-right: 20px;
    }
    .legend-text {
        margin-left: 5px;
    }
    </style>
    
    <div class="legend-container">
        <div class="legend-item">
            <span class="metadata-icon">🔍</span>
            <span class="legend-text">Metadata-Driven Pipeline</span>
        </div>
        <div class="legend-item">
            <span class="manual-icon">📋</span>
            <span class="legend-text">Manual Pipeline</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display tabs for Scheduled and Ad Hoc pipelines
    tab1, tab2 = st.tabs(["Scheduled Pipelines", "Ad Hoc Pipelines"])
    
    with tab1:
        if scheduled_pipelines:
            st.subheader("Scheduled Pipelines")
            for pipeline in scheduled_pipelines:
                # Choose icon based on whether pipeline is metadata-driven
                icon = "🔍" if pipeline['is_metadata_driven'] else "🔄"
                
                # Add a custom CSS class for styling
                expander_class = "metadata-pipeline" if pipeline['is_metadata_driven'] else "standard-pipeline"
                
                with st.expander(f"{icon} {pipeline['name']}", expanded=False):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Source URL:** {pipeline['source_url']}")
                        st.write(f"**Target:** {pipeline['dataset_name']}.{pipeline['target_table']}")
                        
                        # Show metadata selection info if applicable
                        if pipeline['is_metadata_driven']:
                            metadata_type = pipeline['metadata_selection']['type']
                            if metadata_type == "filter":
                                st.write("**Metadata:** Filter-based selection")
                                
                                # Show filter criteria
                                filters = pipeline['metadata_selection'].get('filters', {})
                                filter_text = []
                                if filters.get('source_type'):
                                    filter_text.append(f"Source Type: {filters['source_type']}")
                                if filters.get('logical_name'):
                                    filter_text.append(f"Logical Name: {filters['logical_name']}")
                                if filters.get('database_name'):
                                    filter_text.append(f"Database: {filters['database_name']}")
                                if filters.get('schema_name'):
                                    filter_text.append(f"Schema: {filters['schema_name']}")
                                
                                if filter_text:
                                    st.write(f"**Filter:** {', '.join(filter_text)}")
                                st.write(f"**Load Type:** {pipeline['metadata_selection'].get('load_type', 'full')}")
                            else:
                                object_count = len(pipeline['metadata_selection'].get('object_ids', []))
                                st.write(f"**Metadata:** Explicit selection ({object_count} objects)")
                        
                        schedule_info = pipeline['schedule']
                        if schedule_info:
                            schedule_type = schedule_info.get('type', 'Unknown')
                            if schedule_type == 'interval':
                                interval = schedule_info.get('interval_minutes', 0)
                                st.write(f"**Schedule:** Every {interval} minutes")
                            elif schedule_type == 'daily':
                                start_time = schedule_info.get('start_time', '00:00:00')
                                st.write(f"**Schedule:** Daily at {start_time}")
                            elif schedule_type == 'weekly':
                                start_time = schedule_info.get('start_time', '00:00:00')
                                weekday = schedule_info.get('weekday', 'Monday')
                                st.write(f"**Schedule:** Weekly on {weekday} at {start_time}")
                            else:
                                st.write(f"**Schedule:** {schedule_type}")
                    
                    with col2:
                        # Initialize status in session state if not exists
                        if f"pipeline_{pipeline['id']}_status" not in st.session_state:
                            st.session_state[f"pipeline_{pipeline['id']}_status"] = ""
                        
                        status_key = f"pipeline_{pipeline['id']}_status"
                        
                        # Add details button that links to a pipeline details page
                        if st.button("View Details", key=f"details_{pipeline['id']}"):
                            # We'll need to implement this page
                            st.session_state.current_page = "Pipeline Details"
                            st.session_state.selected_pipeline_id = pipeline['id']
                            st.rerun()
                        
                        if st.button("Run Now", key=f"run_{pipeline['id']}"):
                            creds = st.session_state.get("snowflake_creds")
                            if not creds:
                                st.error("No Snowflake credentials found. Please enter them above.")
                            else:
                                threading.Thread(
                                    target=run_pipeline_thread,
                                    args=(
                                        pipeline['name'],
                                        pipeline['dataset_name'],
                                        pipeline['target_table'],
                                        pipeline['id'],
                                        creds
                                    ),
                                    daemon=True
                                ).start()
                                st.rerun()
                        
                        # Show status
                        status = st.session_state.get(status_key, "")
                        if status:
                            if "Running" in status:
                                st.info(status)
                            elif "Success" in status:
                                st.success(status)
                            else:
                                st.error(status)
        else:
            st.info("No scheduled pipelines found.")
    
    with tab2:
        if ad_hoc_pipelines:
            st.subheader("Ad Hoc Pipelines")
            for pipeline in ad_hoc_pipelines:
                # Choose icon based on whether pipeline is metadata-driven
                icon = "🔍" if pipeline['is_metadata_driven'] else "📋"
                
                with st.expander(f"{icon} {pipeline['name']}"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Source URL:** {pipeline['source_url']}")
                        st.write(f"**Target:** {pipeline['dataset_name']}.{pipeline['target_table']}")
                        st.write(f"**Created:** {pipeline['created_at']}")
                        
                        # Show metadata selection info if applicable
                        if pipeline['is_metadata_driven']:
                            metadata_type = pipeline['metadata_selection']['type']
                            if metadata_type == "filter":
                                st.write("**Metadata:** Filter-based selection")
                                
                                # Show filter criteria
                                filters = pipeline['metadata_selection'].get('filters', {})
                                filter_text = []
                                if filters.get('source_type'):
                                    filter_text.append(f"Source Type: {filters['source_type']}")
                                if filters.get('logical_name'):
                                    filter_text.append(f"Logical Name: {filters['logical_name']}")
                                if filters.get('database_name'):
                                    filter_text.append(f"Database: {filters['database_name']}")
                                if filters.get('schema_name'):
                                    filter_text.append(f"Schema: {filters['schema_name']}")
                                
                                if filter_text:
                                    st.write(f"**Filter:** {', '.join(filter_text)}")
                                st.write(f"**Load Type:** {pipeline['metadata_selection'].get('load_type', 'full')}")
                            else:
                                object_count = len(pipeline['metadata_selection'].get('object_ids', []))
                                st.write(f"**Metadata:** Explicit selection ({object_count} objects)")
                    
                    with col2:
                        # Initialize status in session state if not exists
                        if f"pipeline_{pipeline['id']}_status" not in st.session_state:
                            st.session_state[f"pipeline_{pipeline['id']}_status"] = ""
                        
                        status_key = f"pipeline_{pipeline['id']}_status"
                        
                        # Add details button that links to a pipeline details page
                        if st.button("View Details", key=f"details_{pipeline['id']}"):
                            # We'll need to implement this page
                            st.session_state.current_page = "Pipeline Details"
                            st.session_state.selected_pipeline_id = pipeline['id']
                            st.rerun()
                        
                        if st.button("Run Now", key=f"run_{pipeline['id']}"):
                            creds = st.session_state.get("snowflake_creds")
                            if not creds:
                                st.error("No Snowflake credentials found. Please enter them above.")
                            else:
                                threading.Thread(
                                    target=run_pipeline_thread,
                                    args=(
                                        pipeline['name'],
                                        pipeline['dataset_name'],
                                        pipeline['target_table'],
                                        pipeline['id'],
                                        creds
                                    ),
                                    daemon=True
                                ).start()
                                st.rerun()
                        
                        # Show status
                        status = st.session_state.get(status_key, "")
                        if status:
                            if "Running" in status:
                                st.info(status)
                            elif "Success" in status:
                                st.success(status)
                            else:
                                st.error(status)
        else:
            st.info("No ad hoc pipelines found.") 