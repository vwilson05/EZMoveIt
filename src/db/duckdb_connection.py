import duckdb
import os
from pathlib import Path

# Define database path
DB_PATH = Path(__file__).parent.parent.parent / "data" / "ezmoveit.db"


def get_connection():
    """Get a connection to the DuckDB database."""
    # Ensure the data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def execute_query(query, params=None, fetch=False):
    """Execute a query on the DuckDB database."""
    conn = get_connection()
    try:
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        
        if fetch:
            return result.fetchall()
        return result
    finally:
        conn.close()


def reinitialize_database():
    """Drop and recreate the entire database."""
    try:
        # Close any existing connections
        conn = duckdb.connect(DB_PATH)
        
        # Drop existing tables
        conn.execute("DROP TABLE IF EXISTS pipeline_logs")
        conn.execute("DROP TABLE IF EXISTS pipeline_runs")
        conn.execute("DROP TABLE IF EXISTS pipelines")
        conn.execute("DROP TABLE IF EXISTS source_configs")
        
        # Create source_configs table
        conn.execute("""
            CREATE TABLE source_configs (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT,
                config TEXT
            )
        """)
        
        # Create pipelines table
        conn.execute("""
            CREATE TABLE pipelines (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                source_url TEXT,
                target_table TEXT,
                dataset_name TEXT,
                schedule TEXT,
                last_run_status TEXT,
                source_config TEXT
            )
        """)
        
        # Create pipeline_runs table
        conn.execute("""
            CREATE TABLE pipeline_runs (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER,
                pipeline_name TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status TEXT NOT NULL,
                duration FLOAT,
                rows_processed INTEGER,
                error_message TEXT,
                extract_status TEXT,
                normalize_status TEXT,
                load_status TEXT,
                extract_start_time TIMESTAMP,
                extract_end_time TIMESTAMP,
                normalize_start_time TIMESTAMP,
                normalize_end_time TIMESTAMP,
                load_start_time TIMESTAMP,
                load_end_time TIMESTAMP
            )
        """)
        
        # Create pipeline_logs table
        conn.execute("""
            CREATE TABLE pipeline_logs (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER,
                run_id INTEGER,
                event TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                duration FLOAT,
                log_message TEXT,
                pipeline_name TEXT,
                source_url TEXT,
                target_table TEXT,
                dataset_name TEXT,
                stage TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                row_counts TEXT,
                full_trace_json TEXT
            )
        """)
        
        conn.commit()
    finally:
        conn.close()
