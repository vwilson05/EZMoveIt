import duckdb
import os

# Define database path
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/EZMoveIt.duckdb')

def get_connection():
    """Returns a connection to the DuckDB database."""
    return duckdb.connect(DB_PATH)

def execute_query(query, params=None, fetch=False):
    """
    Executes a query with optional parameters.
    
    Args:
        query (str): The SQL query to execute.
        params (tuple, optional): Parameters for parameterized queries.
        fetch (bool): Whether to fetch results.
    
    Returns:
        list: Query results if fetch=True, otherwise None.
    """
    con = get_connection()
    cur = con.cursor()

    if params:
        cur.execute(query, params)
    else:
        cur.execute(query)

    if fetch:
        result = cur.fetchall()
        con.close()
        return result
    
    con.close()
    return None

