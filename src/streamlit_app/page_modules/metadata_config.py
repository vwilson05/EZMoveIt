import streamlit as st
import pandas as pd
import json
from datetime import datetime
from src.db.duckdb_connection import execute_query
from io import StringIO

def get_next_config_id():
    result = execute_query("SELECT MAX(id) FROM metadata_config", fetch=True)
    return (result[0][0] + 1) if result and result[0][0] is not None else 1

def get_source_defaults():
    """Get default database and schema values for each source type"""
    query = """
    SELECT DISTINCT source_type, database_name, schema_name 
    FROM metadata_config 
    ORDER BY source_type, database_name, schema_name
    """
    results = execute_query(query, fetch=True)
    defaults = {}
    for row in results:
        if row[0] not in defaults:
            defaults[row[0]] = {'database': row[1], 'schema': row[2]}
    return defaults

def metadata_config_page():
    st.title("Metadata Configuration")
    st.caption("Manage metadata configurations for pipeline sources")
    
    # Create tabs for different input methods
    tab1, tab2, tab3 = st.tabs(["Add Single Config", "Batch Upload", "View/Edit Configs"])
    
    # Tab 1: Single Config Form
    with tab1:
        # Source Type Selection (outside form)
        source_type = st.selectbox(
            "Source Type",
            ["SQLServer", "Oracle", "API - Public", "API - Private"],
            key="source_type_selector"
        )
        
        # Driver Type (auto-filled based on source type)
        driver_type = "ODBC+Driver+17+for+SQL+Server" if source_type == "SQLServer" else "N/A"
        
        # Get defaults for the selected source type
        defaults = get_source_defaults().get(source_type, {'database': '', 'schema': ''})
        
        # Show different forms based on source type
        if source_type == "SQLServer":
            # SQL Server form
            with st.form("sqlserver_config_form"):
                st.subheader("SQL Server Configuration")
                
                # Basic Information
                logical_name = st.text_input("Logical Name")
                
                # SQL Server-specific fields
                col1, col2 = st.columns(2)
                with col1:
                    hostname = st.text_input("Hostname")
                    database_name = st.text_input("Database Name", value=defaults.get('database', ''))
                    table_name = st.text_input("Table Name")
                with col2:
                    port = st.text_input("Port", value="1433")
                    schema_name = st.text_input("Schema Name", value=defaults.get('schema', ''))
                
                # Load Configuration
                load_type = st.selectbox("Load Type", ["full", "incremental", "CDC"])
                primary_key = st.text_input("Primary Key")
                
                # Show delta fields only for incremental loads
                delta_column = ""
                delta_value = "1900-01-01"
                if load_type == "incremental":
                    delta_column = st.text_input("Delta Column")
                    delta_value = st.text_input("Delta Value", value="1900-01-01")
                
                # Empty values for API fields
                source_url = ""
                endpoint = ""
                
                if st.form_submit_button("Add SQL Server Configuration"):
                    try:
                        # Create source JSON
                        source_json = {
                            "source_type": "sql_server",
                            "driver_type": driver_type,
                            "connection": {
                                "host": hostname,
                                "port": port,
                                "database": database_name,
                                "schema": schema_name
                            }
                        }
                        
                        # Insert new configuration
                        insert_query = """
                        INSERT INTO metadata_config (
                            id, source_type, driver_type, logical_name, hostname, port,
                            database_name, schema_name, table_name, source_url, endpoint,
                            load_type, primary_key, delta_column, delta_value, source_json,
                            last_load_dt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        execute_query(
                            insert_query,
                            params=(
                                get_next_config_id(), source_type, driver_type, logical_name,
                                hostname, port, database_name, schema_name, table_name,
                                source_url, endpoint, load_type, primary_key, delta_column,
                                delta_value, json.dumps(source_json), "1900-01-01"
                            )
                        )
                        
                        st.success("SQL Server configuration added successfully!")
                        
                    except Exception as e:
                        st.error(f"Error adding configuration: {str(e)}")
        
        elif source_type == "Oracle":
            # Oracle form
            with st.form("oracle_config_form"):
                st.subheader("Oracle Configuration")
                
                # Basic Information
                logical_name = st.text_input("Logical Name")
                
                # Oracle-specific fields
                col1, col2 = st.columns(2)
                with col1:
                    hostname = st.text_input("Hostname")
                    service_name = st.text_input("Service Name", value=defaults.get('database', ''))
                    table_name = st.text_input("Table Name")
                with col2:
                    port = st.text_input("Port", value="1521")
                    schema_name = st.text_input("Schema Name", value=defaults.get('schema', ''))
                
                # Load Configuration
                load_type = st.selectbox("Load Type", ["full", "incremental", "CDC"])
                primary_key = st.text_input("Primary Key")
                
                # Show delta fields only for incremental loads
                delta_column = ""
                delta_value = "1900-01-01"
                if load_type == "incremental":
                    delta_column = st.text_input("Delta Column")
                    delta_value = st.text_input("Delta Value", value="1900-01-01")
                
                # Empty values for API fields
                source_url = ""
                endpoint = ""
                
                # Use service_name for database_name in the database
                database_name = service_name
                
                if st.form_submit_button("Add Oracle Configuration"):
                    try:
                        # Create source JSON
                        source_json = {
                            "source_type": "oracle",
                            "connection": {
                                "host": hostname,
                                "port": port,
                                "service_name": service_name,
                                "schema": schema_name
                            }
                        }
                        
                        # Insert new configuration
                        insert_query = """
                        INSERT INTO metadata_config (
                            id, source_type, driver_type, logical_name, hostname, port,
                            database_name, schema_name, table_name, source_url, endpoint,
                            load_type, primary_key, delta_column, delta_value, source_json,
                            last_load_dt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        execute_query(
                            insert_query,
                            params=(
                                get_next_config_id(), source_type, driver_type, logical_name,
                                hostname, port, database_name, schema_name, table_name,
                                source_url, endpoint, load_type, primary_key, delta_column,
                                delta_value, json.dumps(source_json), "1900-01-01"
                            )
                        )
                        
                        st.success("Oracle configuration added successfully!")
                        
                    except Exception as e:
                        st.error(f"Error adding configuration: {str(e)}")
        
        else:  # API sources
            # API source form
            with st.form("api_config_form"):
                st.subheader(f"{source_type} Configuration")
                
                # Basic Information
                logical_name = st.text_input("Logical Name")
                
                # API-specific fields
                source_url = st.text_input("Source URL")
                endpoint = st.text_input("Endpoint")
                
                # Load Configuration
                load_type = st.selectbox("Load Type", ["full", "incremental", "CDC"])
                primary_key = st.text_input("Primary Key")
                
                # Show delta fields only for incremental loads
                delta_column = ""
                delta_value = "1900-01-01"
                if load_type == "incremental":
                    delta_column = st.text_input("Delta Column")
                    delta_value = st.text_input("Delta Value", value="1900-01-01")
                
                # Empty values for database fields
                hostname = ""
                port = "0"
                database_name = ""
                schema_name = ""
                table_name = ""
                
                if st.form_submit_button("Add API Configuration"):
                    try:
                        # Create source JSON
                        source_json = {
                            "source_type": source_type.lower().replace(" ", "_").replace(" - ", "_"),
                            "connection": {
                                "url": source_url,
                                "endpoint": endpoint
                            }
                        }
                        
                        # Insert new configuration
                        insert_query = """
                        INSERT INTO metadata_config (
                            id, source_type, driver_type, logical_name, hostname, port,
                            database_name, schema_name, table_name, source_url, endpoint,
                            load_type, primary_key, delta_column, delta_value, source_json,
                            last_load_dt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        execute_query(
                            insert_query,
                            params=(
                                get_next_config_id(), source_type, driver_type, logical_name,
                                hostname, port, database_name, schema_name, table_name,
                                source_url, endpoint, load_type, primary_key, delta_column,
                                delta_value, json.dumps(source_json), "1900-01-01"
                            )
                        )
                        
                        st.success("API configuration added successfully!")
                        
                    except Exception as e:
                        st.error(f"Error adding configuration: {str(e)}")
    
    # Tab 2: Batch Upload
    with tab2:
        st.write("Upload a CSV file with metadata configurations")
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                if st.button("Import Configurations"):
                    for _, row in df.iterrows():
                        # Create source JSON for each row
                        source_json = {
                            "source_type": row['SOURCE_TYPE'].lower().replace(" ", "_"),
                            "driver_type": row['DRIVER_TYPE'],
                            "connection": {
                                "host": row['HOSTNAME'],
                                "port": row['PORT'],
                                "database": row['DATABASE_NAME'],
                                "schema": row['SCHEMA_NAME']
                            }
                        }
                        
                        # Insert configuration
                        insert_query = """
                        INSERT INTO metadata_config (
                            id, source_type, driver_type, logical_name, hostname, port,
                            database_name, schema_name, table_name, source_url, endpoint,
                            load_type, primary_key, delta_column, delta_value, source_json,
                            last_load_dt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        execute_query(
                            insert_query,
                            params=(
                                get_next_config_id(), row['SOURCE_TYPE'], row['DRIVER_TYPE'],
                                row['LOGICAL_NAME'], row['HOSTNAME'], row['PORT'],
                                row['DATABASE_NAME'], row['SCHEMA_NAME'], row['TABLE_NAME'],
                                row['SOURCE_URL'], row['ENDPOINT'], row['LOAD_TYPE'],
                                row['PRIMARY_KEY'], row['DELTA_COLUMN'], row['DELTA_VALUE'],
                                json.dumps(source_json), row['LAST_LOAD_DT']
                            )
                        )
                    
                    st.success("Configurations imported successfully!")
                    
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    
    # Tab 3: View/Edit Configurations
    with tab3:
        # Fetch existing configurations
        query = """
            SELECT * FROM metadata_config
            ORDER BY id
        """
        results = execute_query(query, fetch=True)
        
        if results:
                # Convert to dataframe
                df = pd.DataFrame(results, columns=[
                    "ID", "SOURCE_TYPE", "DRIVER_TYPE", "LOGICAL_NAME", "HOSTNAME", "PORT",
                    "DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME", "SOURCE_URL", "ENDPOINT",
                    "LOAD_TYPE", "PRIMARY_KEY", "DELTA_COLUMN", "DELTA_VALUE", "SOURCE_JSON",
                    "LAST_LOAD_DT"
                ])
                
                # Add selection column
                df['Select'] = False
                
                # Create editable dataframe with selection column
                edited_df = st.data_editor(
                    df,
                    hide_index=True,
                    key="metadata_editor",
                    column_config={
                        "Select": st.column_config.CheckboxColumn(
                            "Select",
                            help="Select rows to delete"
                        ),
                        "ID": st.column_config.NumberColumn(
                            "ID",
                            disabled=True
                        ),
                        "SOURCE_JSON": st.column_config.TextColumn(
                            "SOURCE_JSON",
                            disabled=True
                        ),
                        "LAST_LOAD_DT": st.column_config.DatetimeColumn(
                            "LAST_LOAD_DT",
                            disabled=True
                        )
                    }
                )
                
                # Add Save Changes button
                if st.button("Save Changes"):
                    try:
                        # Update all rows that weren't selected for deletion
                        for _, row in edited_df[~edited_df["Select"]].iterrows():
                            update_query = """
                            UPDATE metadata_config
                            SET source_type = ?, driver_type = ?, logical_name = ?,
                                hostname = ?, port = ?, database_name = ?, schema_name = ?,
                                table_name = ?, source_url = ?, endpoint = ?, load_type = ?,
                                primary_key = ?, delta_column = ?, delta_value = ?
                            WHERE id = ?
                            """
                            
                            params = (
                                str(row["SOURCE_TYPE"]),
                                str(row["DRIVER_TYPE"]),
                                str(row["LOGICAL_NAME"]),
                                str(row["HOSTNAME"]),
                                str(row["PORT"]),
                                str(row["DATABASE_NAME"]),
                                str(row["SCHEMA_NAME"]),
                                str(row["TABLE_NAME"]),
                                str(row["SOURCE_URL"]),
                                str(row["ENDPOINT"]),
                                str(row["LOAD_TYPE"]),
                                str(row["PRIMARY_KEY"]),
                                str(row["DELTA_COLUMN"]),
                                str(row["DELTA_VALUE"]),
                                int(row["ID"])
                            )
                            
                            execute_query(update_query, params=params)
                        
                        st.success("Changes saved successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error saving changes: {str(e)}")
                
                # Show delete button only if rows are selected
                if edited_df['Select'].any():
                    if st.button("Delete Selected Rows", type="secondary"):
                        try:
                            # Get IDs of selected rows and convert to Python int
                            to_delete = [int(id) for id in edited_df[edited_df["Select"]]["ID"].tolist()]
                            
                            # Delete selected rows
                            delete_query = "DELETE FROM metadata_config WHERE id = ?"
                            for id in to_delete:
                                execute_query(delete_query, params=(id,))
                            
                            st.success("Selected rows deleted successfully!")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Error deleting rows: {str(e)}")
        else:
                st.write("No configurations found.")

if __name__ == "__main__":
    metadata_config_page() 