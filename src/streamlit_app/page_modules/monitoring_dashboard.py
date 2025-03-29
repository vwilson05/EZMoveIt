import streamlit as st
import pandas as pd
import time
import altair as alt
from datetime import datetime, timedelta
from src.db.duckdb_connection import execute_query
from src.pipelines.dlt_pipeline import run_pipeline_with_creds
import threading
import numpy as np

REFRESH_INTERVAL = 10  # seconds

def load_log_data():
    query = """
    SELECT
        l.id,
        l.pipeline_id,
        l.pipeline_name,
        l.event,
        l.log_message,
        l.duration,
        l.dataset_name,
        l.target_table AS table_name,
        l.source_url,
        l.timestamp
    FROM pipeline_logs l
    ORDER BY l.id DESC
    """
    rows = execute_query(query, fetch=True)
    columns = ["id", "pipeline_id", "pipeline_name", "event", "log_message", "duration", "dataset_name", "table_name", "source_url", "timestamp"]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
        df["row_estimate"] = df["log_message"].str.extract(r'Rows Loaded: (\d+)').astype(float)
    return df

def get_pipeline_configs():
    result = execute_query("SELECT id, name FROM pipelines ORDER BY id DESC", fetch=True)
    return {row[1]: row[0] for row in result}

def display_summary(df):
    total_runs = len(df)
    completed = df[df["event"] == "completed"]
    errors = df[df["event"] == "error"]
    total_rows = completed["row_estimate"].sum()
    avg_duration = completed["duration"].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("✅ Completed Runs", len(completed))
    col2.metric("❌ Failed Runs", len(errors))
    col3.metric("📈 Rows Loaded", f"{int(total_rows):,}")
    col4.metric("⏱ Avg. Duration", f"{avg_duration:.1f} sec" if pd.notnull(avg_duration) else "N/A")

def retry_button(row, pipeline_configs):
    if row["event"] != "error":
        return

    key = f"retry_{row['id']}"
    if st.button(f"🔁 Retry `{row['pipeline_name']}`", key=key):
        st.toast("Retrying pipeline...")

        def rerun():
            creds = st.session_state.get("snowflake_creds", {})
            run_pipeline_with_creds(
                pipeline_name=row["pipeline_name"],
                dataset_name=row["dataset_name"],
                table_name=row["table_name"],
                creds=creds
            )
        threading.Thread(target=rerun, daemon=True).start()

def make_duration_chart(df):
    chart = alt.Chart(df).mark_line(point=True).encode(
        x="timestamp:T",
        y="duration:Q",
        color="pipeline_name:N",
        tooltip=["pipeline_name", "timestamp", "duration"]
    ).properties(title="Pipeline Duration Over Time", height=250)
    return chart

def make_rows_chart(df):
    chart = alt.Chart(df).mark_bar().encode(
        x="timestamp:T",
        y="row_estimate:Q",
        color="pipeline_name:N",
        tooltip=["pipeline_name", "timestamp", "row_estimate"]
    ).properties(title="Rows Loaded per Run", height=250)
    return chart

def get_pipeline_names():
    """Get unique pipeline names for the dropdown."""
    query = "SELECT DISTINCT pipeline_name FROM pipeline_runs ORDER BY pipeline_name"
    results = execute_query(query, fetch=True)
    return [row[0] for row in results]

def get_monitoring_data(filters=None):
    """Fetch monitoring data with optional filters."""
    query = """
    SELECT 
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
        p.target_table
    FROM pipeline_runs pr
    LEFT JOIN pipelines p ON pr.pipeline_id = p.id
    WHERE 1=1
    """
    
    params = []
    if filters:
        if filters.get('pipeline_name'):
            query += " AND pr.pipeline_name = ?"
            params.append(filters['pipeline_name'])
        if filters.get('date_range'):
            start_date = datetime.combine(filters['date_range'][0], datetime.min.time())
            end_date = datetime.combine(filters['date_range'][1], datetime.max.time())
            query += " AND pr.start_time >= ? AND pr.start_time <= ?"
            params.extend([start_date, end_date])
    
    query += " ORDER BY pr.start_time DESC"
    return execute_query(query, params=tuple(params) if params else None, fetch=True)

def monitoring_dashboard():
    st.title("📊 ETL Monitoring Dashboard")
    st.caption("Monitor pipeline performance, reliability, and health across all ETL processes.")
    
    # Filters
    st.subheader("🔍 Filters")
    col1, col2 = st.columns(2)
    
    with col1:
        pipeline_names = get_pipeline_names()
        pipeline_name = st.selectbox(
            "Pipeline Name",
            options=["All"] + pipeline_names,
            index=0
        )
    
    with col2:
        date_range = st.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            max_value=datetime.now()
        )
    
    # Apply filters
    filters = {}
    if pipeline_name and pipeline_name != "All":
        filters['pipeline_name'] = pipeline_name
    if date_range:
        filters['date_range'] = date_range
    
    # Fetch monitoring data
    with st.spinner("Loading monitoring data..."):
        data = get_monitoring_data(filters)
    
    if not data:
        st.info("No monitoring data found for the selected filters.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(data, columns=[
        "pipeline_name", "start_time", "end_time", "status", "duration",
        "rows_processed", "error_message", "extract_status", "normalize_status",
        "load_status", "dataset_name", "target_table"
    ])
    
    # Overall Health Metrics
    st.subheader("🎯 Overall Health")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        success_rate = len(df[df['status'] == 'completed']) / len(df) * 100
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    with col2:
        avg_duration = df[df['duration'].notna()]['duration'].mean()
        st.metric("Avg Duration", f"{avg_duration:.1f}s")
    
    with col3:
        total_rows = df[df['rows_processed'].notna()]['rows_processed'].sum()
        st.metric("Total Rows", f"{total_rows:,}")
    
    with col4:
        failed_runs = len(df[df['status'] == 'failed'])
        st.metric("Failed Runs", failed_runs)
    
    # Pipeline Performance
    st.subheader("⚡ Pipeline Performance")
    
    # Success rate by pipeline
    success_by_pipeline = df.groupby('pipeline_name').agg({
        'status': lambda x: (x == 'completed').mean() * 100,
        'duration': 'mean',
        'rows_processed': 'mean'
    }).reset_index()
    
    success_chart = alt.Chart(success_by_pipeline).mark_bar().encode(
        x='pipeline_name:N',
        y='status:Q',
        tooltip=['pipeline_name', alt.Tooltip('status:Q', format='.1f')]
    ).properties(
        title='Success Rate by Pipeline',
        width='container'
    )
    st.altair_chart(success_chart, use_container_width=True)
    
    # Duration trends
    duration_df = df[['start_time', 'duration', 'pipeline_name']].dropna(subset=['duration'])
    if not duration_df.empty:
        duration_chart = alt.Chart(duration_df).mark_line().encode(
            x='start_time:T',
            y='duration:Q',
            color='pipeline_name:N',
            tooltip=['pipeline_name', 'start_time', alt.Tooltip('duration:Q', format='.1f')]
        ).properties(
            title='Pipeline Duration Trends',
            width='container'
        )
        st.altair_chart(duration_chart, use_container_width=True)
    
    # Failure Analysis
    st.subheader("⚠️ Failure Analysis")
    
    # Failed runs table
    failed_runs = df[df['status'] == 'failed'].copy()
    if not failed_runs.empty:
        failed_runs['start_time'] = pd.to_datetime(failed_runs['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        st.dataframe(
            failed_runs[['pipeline_name', 'start_time', 'error_message']],
            hide_index=True,
            column_config={
                'pipeline_name': 'Pipeline',
                'start_time': 'Time',
                'error_message': 'Error'
            }
        )
    else:
        st.info("No failures found in the selected time period.")
    
    # Stage-wise failure analysis
    stage_failures = pd.DataFrame({
        'Stage': ['Extract', 'Normalize', 'Load'],
        'Failures': [
            len(df[df['extract_status'] == 'failed']),
            len(df[df['normalize_status'] == 'failed']),
            len(df[df['load_status'] == 'failed'])
        ]
    })
    
    stage_chart = alt.Chart(stage_failures).mark_bar().encode(
        x='Stage:N',
        y='Failures:Q',
        tooltip=['Stage', 'Failures']
    ).properties(
        title='Failures by Stage',
        width='container'
    )
    st.altair_chart(stage_chart, use_container_width=True)
    
    # Pipeline Details
    st.subheader("📋 Pipeline Details")
    
    # Summary table
    summary_df = df.groupby('pipeline_name').agg({
        'status': lambda x: (x == 'completed').mean() * 100,
        'duration': ['mean', 'std'],
        'rows_processed': ['mean', 'sum'],
        'status': lambda x: (x == 'failed').sum()
    }).round(2)
    
    # Create a new DataFrame with the correct column names
    summary_df_new = pd.DataFrame({
        'pipeline_name': summary_df.index,
        'Success Rate (%)': summary_df[('status', '<lambda>')],
        'Avg Duration (s)': summary_df[('duration', 'mean')],
        'Duration Std (s)': summary_df[('duration', 'std')],
        'Avg Rows': summary_df[('rows_processed', 'mean')],
        'Total Rows': summary_df[('rows_processed', 'sum')],
        'Failed Runs': summary_df[('status', '<lambda_1>')]
    })
    
    st.dataframe(
        summary_df_new,
        hide_index=True,
        column_config={
            'pipeline_name': 'Pipeline',
            'Success Rate (%)': st.column_config.NumberColumn(format='%.1f'),
            'Avg Duration (s)': st.column_config.NumberColumn(format='%.1f'),
            'Duration Std (s)': st.column_config.NumberColumn(format='%.1f'),
            'Avg Rows': st.column_config.NumberColumn(format='%.0f'),
            'Total Rows': st.column_config.NumberColumn(format='%.0f'),
            'Failed Runs': st.column_config.NumberColumn(format='%.0f')
        }
    )

if __name__ == "__main__":
    monitoring_dashboard()
