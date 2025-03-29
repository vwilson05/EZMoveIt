import duckdb
import os
from pathlib import Path

# Define database path
DB_PATH = Path(__file__).parent.parent.parent / "data" / "EZMoveIt.duckdb"


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
        conn = duckdb.connect(str(DB_PATH))
        
        # Drop tables in dependency order
        conn.execute("DROP TABLE IF EXISTS pipeline_log_relations")
        conn.execute("DROP TABLE IF EXISTS pipeline_logs")
        conn.execute("DROP TABLE IF EXISTS pipeline_runs")
        conn.execute("DROP TABLE IF EXISTS pipelines")
        conn.execute("DROP TABLE IF EXISTS scheduled_jobs")
        conn.execute("DROP TABLE IF EXISTS scheduled_pipelines")
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
        
        # Create pipelines table with additional fields
        conn.execute("""
            CREATE TABLE pipelines (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                source_url TEXT NOT NULL,
                target_table TEXT NOT NULL,
                dataset_name TEXT NOT NULL,
                schedule TEXT,
                last_run_status TEXT,
                source_config TEXT,
                snowflake_target TEXT,
                last_run_log TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                api_key TEXT,
                headers TEXT,
                total_runs INTEGER DEFAULT 0,
                successful_runs INTEGER DEFAULT 0,
                failed_runs INTEGER DEFAULT 0,
                last_successful_run TIMESTAMP,
                last_failed_run TIMESTAMP
            )
        """)
        
        # Create pipeline_runs table
        conn.execute("""
            CREATE TABLE pipeline_runs (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER NOT NULL,
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
                load_end_time TIMESTAMP,
                FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
            )
        """)
        
        # Create pipeline_logs table
        conn.execute("""
            CREATE TABLE pipeline_logs (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER NOT NULL,
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
                full_trace_json TEXT,
                FOREIGN KEY (pipeline_id) REFERENCES pipelines(id),
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(id)
            )
        """)

        # Create pipeline_log_relations table
        conn.execute("""
            CREATE TABLE pipeline_log_relations (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER NOT NULL,
                FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
            )
        """)

        # Create scheduled_jobs table
        conn.execute("""
            CREATE TABLE scheduled_jobs (
                id INTEGER PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                schedule INTEGER NOT NULL, 
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create scheduled_pipelines table
        conn.execute("""
            CREATE TABLE scheduled_pipelines (
                id INTEGER PRIMARY KEY,
                pipeline_name TEXT UNIQUE NOT NULL,
                interval_minutes INTEGER NOT NULL
            )
        """)

        # Create indexes for better query performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id ON pipeline_runs(pipeline_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time ON pipeline_runs(start_time);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id ON pipeline_logs(run_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_logs_event ON pipeline_logs(event);")
        
        conn.commit()
    finally:
        conn.close()
