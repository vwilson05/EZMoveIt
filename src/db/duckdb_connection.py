import duckdb
import os
import importlib
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
    """Drop and recreate the entire database by calling duckdb_init."""
    try:
        # Import and run the duckdb_init module
        from src.db import duckdb_init
        
        # Force reload to ensure we're getting the latest version
        importlib.reload(duckdb_init)
        
        print("Database reinitialized successfully!")
        return True
    except Exception as e:
        print(f"Error reinitializing database: {str(e)}")
        return False
