import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from src.db.duckdb_connection import execute_query
import altair as alt
import time
from src.pipelines.dlt_pipeline import run_pipeline_with_creds
import json
import os
import threading
import logging

REFRESH_INTERVAL = 10  # seconds

def get_progress_for_status(status):
    if status == 'completed':
        return 100
    elif status == 'running':
        return 50
    elif status == 'failed':
        return 100  # Still show full bar but in error styling
    else:  # pending or None
        return 0

def get_pipeline_names():
    """Get unique pipeline names for the dropdown."""
    query = "SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name"
    results = execute_query(query, fetch=True)
    return [row[0] for row in results]

def get_pipeline_runs(filters=None):
    """Fetch pipeline runs with optional filters."""
    query = """
    SELECT 
        pr.id,
        pr.pipeline_name,
        pr.start_time,
        pr.end_time,
        pr.status,
        pr.duration,
        pr.rows_processed,
        pr.error_message,
        pr.extract_status,
        pr.normalize_status,
        pr.load_status,
        p.dataset_name,
        p.target_table,
        p.source_url,
        pr.total_rows,
        pr.total_chunks,
        pr.processed_chunks,
        pr.processed_rows,
        pr.current_chunk,
        pr.estimated_completion
    FROM pipeline_runs pr
    LEFT JOIN pipelines p ON pr.pipeline_id = p.id
    WHERE 1=1
    """
    
    params = []
    if filters:
        if filters.get('pipeline_name'):
            query += " AND pr.pipeline_name = ?"
            params.append(filters['pipeline_name'])
        if filters.get('status'):
            query += " AND pr.status = ?"
            params.append(filters['status'])
        if filters.get('date_range'):
            start_date = datetime.combine(filters['date_range'][0], datetime.min.time())
            end_date = datetime.combine(filters['date_range'][1], datetime.max.time())
            query += " AND pr.start_time >= ? AND pr.start_time <= ?"
            params.extend([start_date, end_date])
    
    query += " ORDER BY pr.start_time DESC"
    return execute_query(query, params=tuple(params) if params else None, fetch=True)

def pipeline_runs_page():
    # Clear any unnecessary session state data
    keys_to_clear = [
        'edit_pipeline', 'pipeline_name', 'config_mode', 'selected_source', 
        'source_config', 'source_url', 'target_table', 'dataset_name', 
        'schedule_option', 'start_time_val', 'interval_minutes', 'selected_source_type',
        'available_databases', 'available_schemas', 'available_tables',
        'selected_database', 'selected_schema', 'selected_tables',
        'load_all_tables', 'configs_to_save', 'default_pipeline_name',
        'default_dataset_name', 'source_config_loaded'
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    
    # Set a flag to prevent pipeline creator content from loading
    st.session_state.current_page = "Pipeline Runs"
    
    st.title("🔄 Pipeline Runs")
    
    # Add a refresh button at the top right
    col1, col2 = st.columns([5, 1])
    with col1:
        st.caption("Track pipeline execution status and performance. Auto-refresh is on.")
    with col2:
        if st.button("🔄 Refresh Now", key="manual_refresh"):
            st.rerun()
    
    # Refresh timer
    st_autorefresh = st.empty()
    st_autorefresh.write("")
    
    # Filters
    st.subheader("🔍 Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        pipeline_names = get_pipeline_names()
        pipeline_name = st.selectbox(
            "Pipeline Name",
            options=["All"] + pipeline_names,
            index=0
        )
    
    with col2:
        status_filter = st.selectbox(
            "Status",
            ["All", "running", "completed", "failed"],
            index=0
        )
    
    with col3:
        date_range = st.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=7), datetime.now()),
            max_value=datetime.now()
        )
    
    # Apply filters
    filters = {}
    if pipeline_name and pipeline_name != "All":
        filters['pipeline_name'] = pipeline_name
    if status_filter != "All":
        filters['status'] = status_filter
    if date_range:
        filters['date_range'] = date_range
    
    # Fetch and display runs
    with st.spinner("Loading pipeline runs..."):
        runs = get_pipeline_runs(filters)
    
    if not runs:
        st.info("No pipeline runs found matching the filters.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(runs, columns=[
        "id", "pipeline_name", "start_time", "end_time", "status",
        "duration", "rows_processed", "error_message", "extract_status",
        "normalize_status", "load_status", "dataset_name", "target_table",
        "source_url", "total_rows", "total_chunks", "processed_chunks",
        "processed_rows", "current_chunk", "estimated_completion"
    ])
    
    # Debug info for chart data
    st.caption(f"Showing data for {len(df)} runs")
    if pipeline_name != "All":
        st.caption(f"Filtered by pipeline: {pipeline_name}")
        st.caption(f"Available data points: {len(df[df['duration'].notna()])} duration records, {len(df[df['rows_processed'].notna()])} rows records")
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Runs", len(df))
    with col2:
        st.metric("Running", len(df[df['status'] == 'running']))
    with col3:
        st.metric("Completed", len(df[df['status'] == 'completed']))
    with col4:
        st.metric("Failed", len(df[df['status'] == 'failed']))
    
    # Display runs table
    st.subheader("📋 Past Pipeline Runs")
    
    # Format the DataFrame for display
    display_df = df.copy()
    display_df['start_time'] = pd.to_datetime(display_df['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    display_df['end_time'] = pd.to_datetime(display_df['end_time']).dt.strftime('%Y-%m-%d %H:%M:%S') if display_df['end_time'].notna().any() else None
    display_df['duration'] = display_df['duration'].round(2) if display_df['duration'].notna().any() else None
    
    # Add status emojis
    display_df['status'] = display_df['status'].map({
        'running': '🔄 Running',
        'completed': '✅ Completed',
        'failed': '❌ Failed'
    })
    
    # Display the table with expandable rows
    for _, row in display_df.iterrows():
        with st.expander(f"{row['pipeline_name']} - {row['status']} ({row['start_time']})"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Pipeline Details**")
                st.markdown(f"- **Dataset:** `{row['dataset_name']}`")
                st.markdown(f"- **Target Table:** `{row['target_table']}`")
                st.markdown(f"- **Duration:** {row['duration']} seconds" if row['duration'] else "- **Duration:** N/A")
                st.markdown(f"- **Rows Processed:** {row['rows_processed']:,}" if row['rows_processed'] else "- **Rows Processed:** N/A")
            
            with col2:
                st.markdown("**Stage Status**")
                st.markdown(f"- **Extract:** {row['extract_status'] or 'N/A'}")
                st.markdown(f"- **Normalize:** {row['normalize_status'] or 'N/A'}")
                st.markdown(f"- **Load:** {row['load_status'] or 'N/A'}")
            
            if row['error_message']:
                st.error(f"**Error:** {row['error_message']}")
            
            # Action buttons
            col1, col2 = st.columns(2)
            with col1:
                if row['status'] == 'running':
                    st.button("Running...", key=f"disabled_{row['id']}", disabled=True)
                else:
                    if st.button(f"Run {row['pipeline_name']}", key=f"trigger_{row['id']}"):
                        # Create a container for pipeline progress
                        progress_container = st.container()
                        
                        # Capture credentials before starting the thread
                        if "snowflake_creds" not in st.session_state:
                            st.error("No Snowflake credentials found. Please set them in the Settings page first.")
                            return
                            
                        # Make a deep copy of credentials to ensure thread safety
                        thread_creds = {
                            'username': st.session_state.snowflake_creds['username'],
                            'password': st.session_state.snowflake_creds['password'],
                            'host': st.session_state.snowflake_creds['host'],
                            'role': st.session_state.snowflake_creds['role'],
                            'database': st.session_state.snowflake_creds['database'],
                            'session_keep_alive': st.session_state.snowflake_creds.get('session_keep_alive', True)
                        }
                        
                        with progress_container:
                            st.markdown("### Pipeline Progress")
                            
                            # Extract stage
                            extract_container = st.empty()
                            extract_status_text = st.empty()
                            
                            # Normalize stage
                            normalize_container = st.empty()
                            normalize_status_text = st.empty()
                            
                            # Load stage
                            load_container = st.empty()
                            load_status_text = st.empty()
                            
                            # Overall status and metrics
                            status_container = st.empty()
                            metrics_container = st.empty()
                            
                            result_container = {"status": "Pipeline started...", "result": None}

                            def run_pipeline_thread(creds):
                                result_container["status"] = "Pipeline started..."
                                logging.info("Thread using credentials: %s", 
                                           {k: v for k, v in creds.items() if k != 'password'})
                                res = run_pipeline_with_creds(row['pipeline_name'], row['dataset_name'], 
                                                           row['target_table'], creds)
                                if res is not None:
                                    result_container["status"] = f"Pipeline completed: {res} rows loaded."
                                    result_container["result"] = res
                                else:
                                    result_container["status"] = "Pipeline failed. Check logs."

                            pipeline_thread = threading.Thread(target=run_pipeline_thread, args=(thread_creds,), daemon=True)
                            pipeline_thread.start()

                            # Update UI with progress
                            while pipeline_thread.is_alive():
                                run_status = execute_query(
                                    """
                                    SELECT status, extract_status, normalize_status, load_status, 
                                           duration, rows_processed
                                    FROM pipeline_runs 
                                    WHERE pipeline_name = ? 
                                    ORDER BY start_time DESC 
                                    LIMIT 1
                                    """,
                                    (row['pipeline_name'],),
                                    fetch=True
                                )
                                
                                if run_status:
                                    status, extract_status, normalize_status, load_status, duration, rows = run_status[0]
                                    
                                    # Update extract progress
                                    extract_container.progress(get_progress_for_status(extract_status))
                                    extract_status_text.markdown(f"**Extract:** {extract_status}")
                                    
                                    # Update normalize progress
                                    normalize_container.progress(get_progress_for_status(normalize_status))
                                    normalize_status_text.markdown(f"**Normalize:** {normalize_status}")
                                    
                                    # Update load progress
                                    load_container.progress(get_progress_for_status(load_status))
                                    load_status_text.markdown(f"**Load:** {load_status}")
                                    
                                    # Update overall status
                                    status_container.info(f"**Status:** {status}")
                                    
                                    # Update metrics if available
                                    if duration and rows:
                                        metrics_container.markdown(f"""
                                            ### 📊 Current Metrics
                                            - **Duration:** {duration:.2f} seconds
                                            - **Rows Processed:** {rows:,}
                                        """)
                                
                                time.sleep(0.5)

                            # Final update
                            if result_container["result"] is not None:
                                extract_status_text.markdown("✅ **Extraction complete**")
                                normalize_status_text.markdown("✅ **Normalization complete**")
                                load_status_text.markdown("✅ **Load complete**")
                                status_container.success(result_container["status"])
                                
                                # Show final metrics
                                final_metrics = execute_query(
                                    """
                                    SELECT duration, rows_processed
                                    FROM pipeline_runs 
                                    WHERE pipeline_name = ? 
                                    ORDER BY start_time DESC 
                                    LIMIT 1
                                    """,
                                    (row['pipeline_name'],),
                                    fetch=True
                                )
                                
                                if final_metrics:
                                    duration, rows = final_metrics[0]
                                    metrics_container.markdown(f"""
                                        ### 📊 Final Metrics
                                        - **Duration:** {duration:.2f} seconds
                                        - **Rows Processed:** {rows:,}
                                        - **Rows/Second:** {rows/duration:.2f}
                                    """)
            
            with col2:
                if st.button("✏️ Edit Pipeline", key=f"edit_{row['id']}"):
                    # Store pipeline details in session state for editing
                    st.session_state.edit_pipeline = {
                        'name': row['pipeline_name'],
                        'dataset_name': row['dataset_name'],
                        'target_table': row['target_table'],
                        'source_url': row['source_url'],
                    }
                    # Switch to Pipeline Editor page
                    st.session_state.current_page = "Pipeline Editor"
                    st.rerun()
    
    # Add charts
    st.subheader("📊 Run Statistics")
    
    # Duration over time chart
    duration_df = df[['start_time', 'duration', 'pipeline_name']].dropna(subset=['duration'])
    if not duration_df.empty:
        duration_chart = alt.Chart(duration_df).mark_line().encode(
            x='start_time:T',
            y='duration:Q',
            tooltip=['pipeline_name', 'start_time', 'duration']
        ).properties(
            title='Pipeline Duration Over Time',
            width='container'
        )
        st.altair_chart(duration_chart, use_container_width=True)
    else:
        st.info("No duration data available for the selected filters.")
    
    # Rows processed chart
    rows_df = df[['start_time', 'rows_processed', 'pipeline_name']].dropna(subset=['rows_processed'])
    if not rows_df.empty:
        rows_chart = alt.Chart(rows_df).mark_bar().encode(
            x='start_time:T',
            y='rows_processed:Q',
            tooltip=['pipeline_name', 'start_time', 'rows_processed']
        ).properties(
            title='Rows Processed Over Time',
            width='container'
        )
        st.altair_chart(rows_chart, use_container_width=True)
    else:
        st.info("No rows processed data available for the selected filters.")
    
    # After the metrics section, add this special section for running pipelines
    running_pipelines = df[df['status'] == 'running']
    if not running_pipelines.empty:
        st.subheader("🏃‍♂️ Currently Running Pipelines")
        
        # Custom styling for progress bars
        st.markdown("""
            <style>
            .stProgress > div > div > div > div {
                background-color: #1f77b4;  /* Extract color */
            }
            .stProgress > div > div > div > div:nth-child(2) {
                background-color: #2ca02c;  /* Normalize color */
            }
            .stProgress > div > div > div > div:nth-child(3) {
                background-color: #ff7f0e;  /* Load color */
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Create a progress tracker for each running pipeline
        for _, row in running_pipelines.iterrows():
            with st.expander(f"🔄 {row['pipeline_name']} - Running", expanded=True):
                st.markdown(f"**Dataset:** `{row['dataset_name']}` | **Target Table:** `{row['target_table']}`")
                
                # Add chunk progress information
                if row['total_chunks'] and row['total_chunks'] > 0:
                    # Calculate percentage and ensure it's between 0-100
                    chunk_percent = min(100, int((row['processed_chunks'] / row['total_chunks']) * 100))
                    
                    # Display progress bar
                    progress_container = st.container()
                    with progress_container:
                        st.progress(chunk_percent / 100)
                        
                        # Create two columns for metrics
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Progress", f"{chunk_percent}%")
                            st.metric("Chunks", f"{row['processed_chunks']} of {row['total_chunks']}")
                        
                        with col2:
                            st.metric("Rows Processed", f"{row['processed_rows']:,} of {row['total_rows']:,}")
                            
                            # Calculate and show rate if we have processed rows
                            if row['processed_rows'] > 0 and row['start_time']:
                                start_time = pd.to_datetime(row['start_time'])
                                elapsed_seconds = (datetime.now() - start_time).total_seconds()
                                if elapsed_seconds > 0:
                                    rate = row['processed_rows'] / elapsed_seconds
                                    st.metric("Rows/Second", f"{rate:.1f}")
                    
                    # Show estimated completion time if available
                    if row['estimated_completion']:
                        est_time = pd.to_datetime(row['estimated_completion'])
                        time_remaining = (est_time - datetime.now()).total_seconds()
                        if time_remaining > 0:
                            minutes, seconds = divmod(int(time_remaining), 60)
                            hours, minutes = divmod(minutes, 60)
                            time_str = f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes}m {seconds}s"
                            st.info(f"⏱️ Estimated completion in: {time_str}")
                
                # Extract progress
                extract_status = row['extract_status'] or 'pending'
                extract_container = st.empty()
                extract_progress = get_progress_for_status(extract_status)
                extract_container.progress(extract_progress)
                st.markdown(f"**Extract:** {extract_status}")
                
                # Normalize progress
                normalize_status = row['normalize_status'] or 'pending'
                normalize_container = st.empty()
                normalize_progress = get_progress_for_status(normalize_status)
                normalize_container.progress(normalize_progress)
                st.markdown(f"**Normalize:** {normalize_status}")
                
                # Load progress
                load_status = row['load_status'] or 'pending'
                load_container = st.empty()
                load_progress = get_progress_for_status(load_status)
                load_container.progress(load_progress)
                st.markdown(f"**Load:** {load_status}")
                
                # Show start time and running duration
                if row['start_time']:
                    start_time = pd.to_datetime(row['start_time'])
                    current_time = datetime.now()
                    running_duration = (current_time - start_time).total_seconds()
                    st.markdown(f"**Started at:** {start_time.strftime('%Y-%m-%d %H:%M:%S')} | **Running for:** {running_duration:.2f} seconds")
                
                # Show any rows processed so far if available
                if row['rows_processed']:
                    st.markdown(f"**Rows processed so far:** {row['rows_processed']:,}")
    
    # Auto-refresh using JavaScript to avoid flickering
    st.markdown(
        f"""
        <script>
            setTimeout(function() {{
                window.location.reload();
            }}, {REFRESH_INTERVAL * 1000});
        </script>
        """,
        unsafe_allow_html=True
    ) 