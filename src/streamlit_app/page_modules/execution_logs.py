import streamlit as st
from src.db.duckdb_connection import execute_query


def fetch_execution_logs():
    """Retrieve execution logs from DuckDB, including all relevant fields."""
    query = """
    SELECT id, pipeline_id, pipeline_name, source_url, target_table, dataset_name,
           event, created_at, log_message
    FROM pipeline_logs ORDER BY created_at DESC
    """
    return execute_query(query, fetch=True)


def execution_logs_page():
    st.title("📜 Pipeline Execution Logs")

    logs = fetch_execution_logs()

    if logs:
        for log in logs:
            (
                log_id,
                pipeline_id,
                pipeline_name,
                source_url,
                target_table,
                dataset_name,
                event,
                timestamp,
                message,
            ) = log

            st.markdown(f"🆔 `{log_id}` | **Pipeline:** `{pipeline_name}`")
            st.markdown(f"🔄 **Status:** `{event}` | 🕒 `{timestamp}`")
            st.markdown(f"📌 **Source URL:** `{source_url}`")
            st.markdown(
                f"🎯 **Target Table:** `{target_table}` | 🏛 **Schema:** `{dataset_name}`"
            )
            st.markdown(f"📝 **Log Message:** {message}")
            st.markdown("---")
    else:
        st.info("🚀 No logs found yet.")
