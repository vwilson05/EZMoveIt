import logging
import json
import os
from sqlalchemy import create_engine, text
import dlt
import pendulum
from dlt.sources.sql_database import sql_table, sql_database
from itertools import islice
import time

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../../config")

def paginate_generator(gen, chunk_size=50000):
    """Yield chunks (lists) of rows from the generator."""
    while True:
        chunk = list(islice(gen, chunk_size))
        if not chunk:
            break
        yield chunk

def load_db_config(pipeline_name):
    config_path = os.path.join(
        CONFIG_DIR, f"{pipeline_name.replace(' ', '_').lower()}_config.json"
    )
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}

def fetch_data_from_database(pipeline_name, run_id=None):
    """
    Returns a DLT source/resource for SQL database access with progress tracking.
    """
    db_config = load_db_config(pipeline_name)
    if not db_config:
        logging.error(f"❌ No database config found for `{pipeline_name}`!")
        return None

    # Get performance settings from config
    use_parallel = db_config.get("use_parallel", True)
    chunk_size = db_config.get("chunk_size", 50000)

    # Build connection string.
    conn_str = db_config.get("credentials")
    if not conn_str:
        db_type = db_config.get("db_type", "").lower()
        user = db_config.get("user", "loader")
        password = db_config.get("password", "loader")
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 1433 if db_type in ["mssql", "microsoft_sqlserver"] else 1521)
        database = db_config.get("database", "dlt_data")
        if db_type in ["mssql", "microsoft_sqlserver"]:
            driver = db_config.get("driver", "ODBC+Driver+17+for+SQL+Server")
            conn_str = (
                f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}"
                f"?TrustServerCertificate=yes&driver={driver}"
            )
            logging.info(f"Built SQL Server connection string: {conn_str}")
        elif db_type == "oracle":
            service_name = db_config.get("service_name", "orcl")
            conn_str = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
            logging.info(f"Built Oracle connection string: {conn_str}")
        else:
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            logging.info(f"Built PostgreSQL connection string: {conn_str}")
    else:
        logging.info(f"Using provided connection string: {conn_str}")

    engine = create_engine(conn_str)
    logging.info("SQLAlchemy engine created.")

    # Incremental settings.
    incremental_type = db_config.get("incremental_type", "FULL").upper()
    primary_key = db_config.get("primary_key")
    delta_column = db_config.get("delta_column")
    delta_value = db_config.get("delta_value")

    mode = db_config.get("mode", "sql_table")
    schema_name = db_config.get("schema")

    if mode == "sql_table":
        table_name = db_config.get("table")
        if not table_name:
            logging.error("❌ No table name specified in config for single table mode!")
            return None

        # Count total rows to calculate chunks
        if run_id:
            try:
                # Create a fresh connection to count rows
                count_query_str = f"SELECT COUNT(*) FROM {schema_name}.{table_name}" if schema_name else f"SELECT COUNT(*) FROM {table_name}"
                count_query = text(count_query_str)
        
                with engine.connect() as conn:
                    row_count = conn.execute(count_query).scalar()
                
                # Calculate total chunks
                total_chunks = (row_count + chunk_size - 1) // chunk_size  # Ceiling division
                
                # Seconds to add for estimated completion (total_chunks * seconds_per_chunk)
                seconds_to_add = total_chunks * 2  # Assume 2 seconds per chunk as baseline
                
                # Update pipeline run with row and chunk counts using direct SQL
                from src.db.duckdb_connection import execute_query
                update_query = f"""
                UPDATE pipeline_runs 
                SET total_rows = {row_count}, 
                    total_chunks = {total_chunks},
                    estimated_completion = CURRENT_TIMESTAMP + INTERVAL '{seconds_to_add}' SECOND
                WHERE id = {run_id}
                """
                execute_query(update_query)
                
                logging.info(f"Total rows: {row_count}, Total chunks: {total_chunks}")
            except Exception as e:
                logging.error(f"Error counting rows: {str(e)}")

        logging.info(f"Configuring sql_table for {schema_name}.{table_name}" if schema_name else f"Configuring sql_table for {table_name}")
        if incremental_type == "INCREMENTAL" and delta_column and delta_value and primary_key:
            initial_dt = pendulum.parse(delta_value)
            res = sql_table(
                engine,
                table=table_name,
                schema=schema_name,
                incremental=dlt.sources.incremental(delta_column, initial_value=initial_dt)
            ).apply_hints(
                primary_key=primary_key,
                batch_size=chunk_size  # Add batch size hint
            )
            if use_parallel:
                res = res.parallelize()
        else: 
            res = sql_table(engine, table=table_name, schema=schema_name)
            if use_parallel:
                res = res.parallelize()
        
        # Create a wrapper around paginate_generator to report progress
        def progress_tracked_generator(resource, chunk_size, total_rows, run_id):
            """Wrap a generator with progress tracking."""
            chunk_counter = 0
            row_counter = 0
            max_rows = total_rows * 1.1 if total_rows else float('inf')  # Add a 10% buffer
            
            # Get the generator
            gen = resource
            total_chunks = (total_rows + chunk_size - 1) // chunk_size if total_rows else None
            
            logging.info(f"Starting chunked processing. Expected: {total_rows} rows, {total_chunks} chunks. Max rows set to: {max_rows}")
            
            while True:
                start_time = time.time()
                
                # Safety check - stop processing if we've exceeded max rows
                if row_counter >= max_rows:
                    logging.warning(f"Reached maximum row limit ({max_rows}). Stopping processing to prevent infinite loop.")
                    break
                    
                chunk = list(islice(gen, chunk_size))
                if not chunk:
                    logging.info("No more data from generator. Stopping processing.")
                    break
                
                chunk_size_actual = len(chunk)
                chunk_counter += 1
                row_counter += chunk_size_actual
                
                # Calculate progress percentage
                progress = (row_counter / total_rows * 100) if total_rows else None
                processing_time = time.time() - start_time
                
                # Log detailed info about this chunk
                logging.info(f"Chunk {chunk_counter}: {chunk_size_actual} rows, {processing_time:.2f}s, Total so far: {row_counter} rows" + 
                             (f" ({progress:.1f}%)" if progress else ""))
                
                # Additional safety check - if we get a very small chunk and are already over expected count
                if chunk_size_actual < chunk_size * 0.1 and row_counter > total_rows:
                    logging.warning(f"Received small chunk ({chunk_size_actual} rows) after processing expected data volume. Likely end of data.")
                    yield chunk
                    break
                    
                # Update progress in database if run_id is provided
                if run_id:
                    try:
                        from src.db.duckdb_connection import execute_query
                        
                        # If we've exceeded the expected rows, update total_rows with current count
                        current_total_rows = row_counter if row_counter > total_rows else total_rows
                        current_total_chunks = (current_total_rows + chunk_size - 1) // chunk_size
                        
                        # Calculate a better estimate of completion time
                        remaining_rows = max(0, current_total_rows - row_counter)
                        
                        # Use actual processing time of this chunk to estimate completion
                        time_per_row = processing_time / chunk_size_actual if chunk_size_actual > 0 else 0
                        estimated_remaining_seconds = int(remaining_rows * time_per_row)
                        
                        # Use direct string formatting instead of parameters
                        update_query = f"""
                        UPDATE pipeline_runs 
                        SET processed_chunks = {chunk_counter},
                            processed_rows = {row_counter},
                            current_chunk = {chunk_counter},
                            total_rows = {current_total_rows},
                            total_chunks = {current_total_chunks},
                            estimated_completion = CURRENT_TIMESTAMP + INTERVAL '{estimated_remaining_seconds}' SECOND
                        WHERE id = {run_id}
                        """
                        execute_query(update_query)
                    except Exception as e:
                        logging.error(f"Error updating progress: {str(e)}")
                
                yield chunk
            
            # Final update and logging
            logging.info(f"Processing complete. Total: {row_counter} rows in {chunk_counter} chunks.")
            if run_id:
                try:
                    from src.db.duckdb_connection import execute_query
                    # Use direct string formatting for the final update too
                    final_update_query = f"""
                    UPDATE pipeline_runs 
                    SET processed_chunks = {chunk_counter},
                        processed_rows = {row_counter},
                        current_chunk = {chunk_counter},
                        total_rows = {row_counter},  -- Update to actual number processed
                        status = 'completed'         -- Mark as complete
                    WHERE id = {run_id}
                    """
                    execute_query(final_update_query)
                except Exception as e:
                    logging.error(f"Error updating final progress: {str(e)}")
        
        # Return the progress-tracked generator instead
        return progress_tracked_generator(res, chunk_size, row_count, run_id)

    elif mode == "sql_database":
        if not schema_name:
            logging.error("❌ Schema is required in sql_database mode!")
            return None
        logging.info(f"Loading schema '{schema_name}' from database.")
        source = sql_database(engine, schema=schema_name)
        if use_parallel:
            source = source.parallelize()
        table_list = db_config.get("tables")
        if table_list and len(table_list) > 0:
            logging.info(f"Selecting table subset: {table_list}")
            source = source.with_resources(*table_list)
        else:
            logging.info("No specific table subset provided; loading all tables in schema.")

        if incremental_type == "INCREMENTAL" and delta_column and delta_value and primary_key:
            initial_dt = pendulum.parse(delta_value)
            for tbl in source.resources:
                logging.info(f"Applying incremental hint to table '{tbl}' on column '{delta_column}'")
                source.resources[tbl] = source.resources[tbl].apply_hints(
                    primary_key=primary_key,
                    incremental=dlt.sources.incremental(delta_column, initial_value=initial_dt)
                )
        # Instead of wrapping with a new function object, modify each resource's __call__ method in place.
        for tbl, resource in source.resources.items():
            original_call = resource.__call__
            def new_call(*args, _orig=original_call, **kwargs):
                return paginate_generator(_orig(*args, **kwargs), 50000)
            resource.__call__ = new_call
        return source

    else:
        logging.error("❌ Unknown mode specified in db_config")
        return None

if __name__ == "__main__":
    test_pipeline_name = "test_pipeline"
    resource = fetch_data_from_database(test_pipeline_name)
    if resource:
        logging.info("Successfully fetched database source.")
    else:
        logging.error("Failed to fetch database source.")
