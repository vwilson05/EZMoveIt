import streamlit as st
import pandas as pd
import time
import altair as alt
from datetime import datetime, timedelta
from src.db.duckdb_connection import execute_query
from src.pipelines.dlt_pipeline import run_pipeline_with_creds
import threading

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
        l.snowflake_target AS table_name,
        l.source_url,
        l.created_at
    FROM pipeline_logs l
    ORDER BY l.id DESC
    """
    rows = execute_query(query, fetch=True)
    columns = ["id", "pipeline_id", "pipeline_name", "event", "log_message", "duration", "dataset_name", "table_name", "source_url", "created_at"]
    df = pd.DataFrame(rows, columns=columns)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"])
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
        x="created_at:T",
        y="duration:Q",
        color="pipeline_name:N",
        tooltip=["pipeline_name", "created_at", "duration"]
    ).properties(title="Pipeline Duration Over Time", height=250)
    return chart

def make_rows_chart(df):
    chart = alt.Chart(df).mark_bar().encode(
        x="created_at:T",
        y="row_estimate:Q",
        color="pipeline_name:N",
        tooltip=["pipeline_name", "created_at", "row_estimate"]
    ).properties(title="Rows Loaded per Run", height=250)
    return chart

def monitoring_dashboard_page():
    st.title("📊 Monitoring Dashboard")
    st.caption("Track pipeline health, durations, and run status. Auto-refresh is on.")

    # Refresh timer
    st_autorefresh = st.empty()
    st_autorefresh.write("")

    with st.spinner("Loading pipeline logs..."):
        df_logs = load_log_data()

    if df_logs.empty:
        st.warning("No pipeline logs available yet.")
        return

    pipeline_configs = get_pipeline_configs()

    # Filter UI
    pipelines = sorted(df_logs["pipeline_name"].unique())
    status = sorted(df_logs["event"].unique())
    selected_pipelines = st.multiselect("🔀 Filter Pipelines", options=pipelines, default=pipelines)
    selected_status = st.multiselect("📌 Filter Status", options=status, default=status)

    df_filtered = df_logs[df_logs["pipeline_name"].isin(selected_pipelines)]
    df_filtered = df_filtered[df_filtered["event"].isin(selected_status)]

    # Show Summary
    display_summary(df_filtered)

    # Charts
    with st.expander("📈 View Charts", expanded=False):
        st.altair_chart(make_duration_chart(df_filtered[df_filtered["event"] == "completed"]), use_container_width=True)
        st.altair_chart(make_rows_chart(df_filtered[df_filtered["event"] == "completed"]), use_container_width=True)

    # Per-pipeline detail
    for pipeline_name in selected_pipelines:
        df_pipeline = df_filtered[df_filtered["pipeline_name"] == pipeline_name]
        with st.expander(f"📂 `{pipeline_name}` ({len(df_pipeline)} logs)"):
            st.dataframe(df_pipeline[[
                "created_at", "event", "table_name", "duration", "row_estimate", "log_message"
            ]], use_container_width=True)
            # Retry last failed run (if applicable)
            last_error = df_pipeline[df_pipeline["event"] == "error"].head(1)
            if not last_error.empty:
                retry_button(last_error.iloc[0], pipeline_configs)

    # Auto-refresh
    st_autorefresh.markdown(f"<script>setTimeout(() => window.location.reload(), {REFRESH_INTERVAL * 1000})</script>", unsafe_allow_html=True)

if __name__ == "__main__":
    monitoring_dashboard_page()
