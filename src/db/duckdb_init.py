import duckdb
import os

# Ensure the data directory exists
DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')
os.makedirs(DATA_DIR, exist_ok=True)

# Define database path
DB_PATH = os.path.join(DATA_DIR, 'EZMoveIt.duckdb')

# Connect to DuckDB
con = duckdb.connect(DB_PATH)

# Drop tables in dependency order
con.execute("DROP TABLE IF EXISTS pipeline_log_relations")
con.execute("DROP TABLE IF EXISTS pipeline_logs")
con.execute("DROP TABLE IF EXISTS pipelines")
con.execute("DROP TABLE IF EXISTS scheduled_jobs")
con.execute("DROP TABLE IF EXISTS scheduled_pipelines")

# ✅ Ensure `id` and `pipeline_id` are both `BIGINT`
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
    headers TEXT
);
""")

# ✅ Remove `id` primary key from `pipeline_logs`
con.execute("""
CREATE TABLE pipeline_logs (
    id INTEGER NOT NULL,  -- We will manually assign this
    pipeline_id INTEGER NOT NULL,  
    event TEXT NOT NULL,  
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    duration FLOAT,
    log_message TEXT,
    pipeline_name TEXT,
    source_url TEXT,
    snowflake_target TEXT,
    dataset_name TEXT
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

con.close()

print("✅ DuckDB database schema reset successfully!")
