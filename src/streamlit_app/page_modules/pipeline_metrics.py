import sys
import os

# Ensure project root is always in sys.path (fixes disappearing imports)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)  # ✅ Use insert(0) to give it priority

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import re  # ✅ Import regex for better string parsing

# ✅ Import DuckDB connection
from src.db.duckdb_connection import execute_query 


def fetch_pipeline_metrics():
    """Retrieve and parse pipeline execution logs from DuckDB."""
    query = """
    SELECT pipeline_name, source_url, target_table, event, timestamp, log_message, duration
    FROM pipeline_logs 
    WHERE event in ('completed', 'failed')
    ORDER BY timestamp DESC
    """
    logs = execute_query(query, fetch=True)

    if not logs:
        return pd.DataFrame(
            columns=[
                "Pipeline",
                "Source",
                "Target",
                "Event",
                "Timestamp",
                "Duration (s)",
                "Rows Loaded",
            ]
        )

    records = []
    for (
        pipeline_name,
        source_url,
        target_table,
        event,
        timestamp,
        message,
        duration,
    ) in logs:
        rows_loaded = None

        # Extract "Rows Loaded" value using regex
        match = re.search(r"Rows Loaded: (\d+)", message)
        if match:
            rows_loaded = int(match.group(1))

        # Ensure duration is properly handled (from DB or parsed)
        parsed_duration = float(duration) if duration else None

        records.append(
            {
                "Pipeline": pipeline_name,
                "Source": source_url,
                "Target": target_table,
                "Event": event,
                "Timestamp": timestamp,
                "Duration (s)": parsed_duration,
                "Rows Loaded": rows_loaded,
            }
        )

    return pd.DataFrame(records)


def pipeline_metrics_page():
    """Retrieve and parse pipeline execution logs from DuckDB."""

    query = """
    SELECT pipeline_name, source_url, target_table, event, timestamp, log_message, duration 
    FROM pipeline_logs 
    ORDER BY timestamp DESC
    """

    logs = execute_query(query, fetch=True)

    if not logs:
        return pd.DataFrame(
            columns=[
                "Pipeline",
                "Source",
                "Target",
                "Event",
                "Timestamp",
                "Duration (s)",
                "Rows Loaded",
            ]
        )

    records = []
    for (
        pipeline_name,
        source_url,
        target_table,
        event,
        timestamp,
        message,
        duration,
    ) in logs:
        rows_loaded = None

        if "Rows Loaded:" in message:
            try:
                parts = message.split(" ")
                if parts[-1].isdigit():
                    rows_loaded = int(parts[-1])
            except Exception as e:
                st.warning(f"⚠️ Error parsing log: {message} | {str(e)}")

        records.append(
            {
                "Pipeline": pipeline_name,
                "Source": source_url,
                "Target": target_table,
                "Event": event,
                "Timestamp": timestamp,
                "Duration (s)": duration,  # Use stored duration directly
                "Rows Loaded": rows_loaded,
            }
        )

    return pd.DataFrame(records)


# --- **Streamlit UI** ---
st.title("📊 Pipeline Metrics & Data Explorer")

# ✅ **Retrieve Data**
df_metrics = fetch_pipeline_metrics()

# ✅ **Check for Empty Data**
if df_metrics.empty:
    st.warning("🚀 No pipeline runs found. Trigger a pipeline first!")
    st.stop()

# ✅ **Show All Data**
st.header("📜 Pipeline Logs")
st.dataframe(df_metrics, use_container_width=True)

# ✅ **Execution Time Over Runs**
st.header("📈 Execution Time Over Runs")

if not df_metrics.empty:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(
        df_metrics["Timestamp"],
        df_metrics["Duration (s)"],
        marker="o",
        linestyle="-",
        label="Execution Time (s)",
    )
    ax.set_xlabel("Run Timestamp")
    ax.set_ylabel("Duration (s)")
    ax.set_title("Pipeline Execution Duration Over Time")
    ax.legend()
    plt.xticks(rotation=45)
    st.pyplot(fig)
else:
    st.warning("⚠️ No completed runs to chart.")

# ✅ **Rows Loaded Per Run (Bar Chart)**
st.header("📊 Rows Loaded Per Run")

if not df_metrics.empty:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(
        df_metrics["Timestamp"],
        df_metrics["Rows Loaded"],
        color="skyblue",
        label="Rows Loaded",
    )
    ax.set_xlabel("Run Timestamp")
    ax.set_ylabel("Rows Loaded")
    ax.set_title("Rows Loaded Per Run")
    ax.legend()
    plt.xticks(rotation=45)
    st.pyplot(fig)
else:
    st.warning("⚠️ No completed runs to chart.")

# ✅ **Success vs. Error Rate (Pie Chart)**
st.header("📊 Pipeline Success vs. Errors")

if not df_metrics.empty:
    success_count = (df_metrics["Event"] == "completed").sum()
    error_count = (df_metrics["Event"] == "error").sum()

    if success_count == 0 and error_count == 0:
        st.warning("⚠️ No success/error data to display.")
    else:
        fig, ax = plt.subplots(figsize=(4, 4))
        ax.pie(
            [success_count, error_count],
            labels=["Success", "Error"],
            autopct="%1.1f%%",
            colors=["green", "red"],
        )
        ax.set_title("Pipeline Success Rate")
        st.pyplot(fig)
else:
    st.warning("⚠️ No completed runs to analyze.")
st.rerun()