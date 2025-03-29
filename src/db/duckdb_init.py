import duckdb
import os

# Ensure the data directory exists
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data")
os.makedirs(DATA_DIR, exist_ok=True)

# Define database path
DB_PATH = os.path.join(DATA_DIR, "EZMoveIt.duckdb")

# Connect to DuckDB
con = duckdb.connect(DB_PATH)

# Drop tables in dependency order
con.execute("DROP TABLE IF EXISTS pipeline_log_relations")
con.execute("DROP TABLE IF EXISTS pipeline_logs")
con.execute("DROP TABLE IF EXISTS pipeline_runs")
con.execute("DROP TABLE IF EXISTS pipelines")
con.execute("DROP TABLE IF EXISTS scheduled_jobs")
con.execute("DROP TABLE IF EXISTS scheduled_pipelines")

# Create pipelines table with additional fields
con.execute("""
CREATE TABLE pipelines (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    source_url TEXT,
    snowflake_target TEXT,
    schedule TEXT,           
    last_run_status TEXT,
    last_run_log TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataset_name TEXT,
    api_key TEXT,
    headers TEXT,
    total_runs INTEGER DEFAULT 0,
    successful_runs INTEGER DEFAULT 0,
    failed_runs INTEGER DEFAULT 0,
    last_successful_run TIMESTAMP,
    last_failed_run TIMESTAMP
);
""")

# Create pipeline_runs table for detailed run tracking
con.execute("""
CREATE TABLE pipeline_runs (
    id INTEGER PRIMARY KEY,
    pipeline_id INTEGER NOT NULL,
    pipeline_name TEXT NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
);
""")

# Create pipeline_logs table with additional fields
con.execute("""
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
);
""")

con.execute("""
CREATE TABLE pipeline_log_relations (
    id INTEGER PRIMARY KEY,
    pipeline_id INTEGER NOT NULL,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);
""")

con.execute("""
CREATE TABLE scheduled_jobs (
    id INTEGER PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    schedule INTEGER NOT NULL, 
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

con.execute("""
CREATE TABLE scheduled_pipelines (
    id INTEGER PRIMARY KEY,
    pipeline_name TEXT UNIQUE NOT NULL,
    interval_minutes INTEGER NOT NULL
);
""")

# Create indexes for better query performance
con.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id ON pipeline_runs(pipeline_id);")
con.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);")
con.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time ON pipeline_runs(start_time);")
con.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id ON pipeline_logs(run_id);")
con.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_logs_event ON pipeline_logs(event);")

con.close()

print("✅ DuckDB database schema reset successfully!")
