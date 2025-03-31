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
        created_at
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
        pipeline_id, name, source_url, target_table, dataset_name, schedule_json, created_at = pipeline
        
        # Parse the schedule if it exists
        schedule = None
        if schedule_json:
            try:
                schedule = json.loads(schedule_json)
            except:
                schedule = None
        
        pipeline_data = {
            "id": pipeline_id,
            "name": name,
            "source_url": source_url,
            "target_table": target_table,
            "dataset_name": dataset_name,
            "created_at": created_at,
            "schedule": schedule
        }
        
        if schedule and schedule.get('type', '').lower() != 'manual':
            scheduled_pipelines.append(pipeline_data)
        else:
            ad_hoc_pipelines.append(pipeline_data)
    
    # Display tabs for Scheduled and Ad Hoc pipelines
    tab1, tab2 = st.tabs(["Scheduled Pipelines", "Ad Hoc Pipelines"])
    
    with tab1:
        if scheduled_pipelines:
            st.subheader("Scheduled Pipelines")
            for pipeline in scheduled_pipelines:
                with st.expander(f"🔄 {pipeline['name']}"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Source URL:** {pipeline['source_url']}")
                        st.write(f"**Target:** {pipeline['dataset_name']}.{pipeline['target_table']}")
                        
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
                with st.expander(f"📋 {pipeline['name']}"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Source URL:** {pipeline['source_url']}")
                        st.write(f"**Target:** {pipeline['dataset_name']}.{pipeline['target_table']}")
                        st.write(f"**Created:** {pipeline['created_at']}")
                    
                    with col2:
                        # Initialize status in session state if not exists
                        if f"pipeline_{pipeline['id']}_status" not in st.session_state:
                            st.session_state[f"pipeline_{pipeline['id']}_status"] = ""
                        
                        status_key = f"pipeline_{pipeline['id']}_status"
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