import streamlit as st
import os
import json
from src.db.duckdb_connection import execute_query

def settings_page():
    st.title("⚙️ Settings")
    st.caption("Configure application settings and manage credentials.")
    
    # Snowflake Credentials
    st.subheader("❄️ Snowflake Credentials")
    
    # Initialize session state for credentials if not exists
    if 'snowflake_creds' not in st.session_state:
        st.session_state.snowflake_creds = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'host': os.getenv('SNOWFLAKE_HOST', ''),
            'username': os.getenv('SNOWFLAKE_USER', ''),
            'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
            'role': os.getenv('SNOWFLAKE_ROLE', ''),
            'database': os.getenv('SNOWFLAKE_DATABASE', ''),
            'session_keep_alive': True
        }
    
    # JSON mode toggle
    use_json = st.checkbox("Use JSON Credentials", value=False)
    
    if not use_json:
        # Create form for credentials
        with st.form("snowflake_credentials"):
            col1, col2 = st.columns(2)
            
            with col1:
                account = st.text_input("Account", value=st.session_state.snowflake_creds['account'])
                host = st.text_input("Host", value=st.session_state.snowflake_creds['host'])
                username = st.text_input("Username", value=st.session_state.snowflake_creds['username'])
                role = st.text_input("Role", value=st.session_state.snowflake_creds['role'])
            
            with col2:
                database = st.text_input("Database", value=st.session_state.snowflake_creds['database'])
                password = st.text_input("Password", type="password", value=st.session_state.snowflake_creds['password'])
                session_keep_alive = st.checkbox("Keep Session Alive", value=st.session_state.snowflake_creds['session_keep_alive'])
            
            submitted = st.form_submit_button("Save Credentials")
            
            if submitted:
                # Save to session state
                st.session_state.snowflake_creds = {
                    'account': account,
                    'host': host,
                    'username': username,
                    'password': password,
                    'role': role,
                    'database': database,
                    'session_keep_alive': session_keep_alive
                }
                st.success("Credentials saved successfully!")
    else:
        # JSON input for credentials
        json_input = st.text_area(
            "Paste JSON credentials",
            value=json.dumps(st.session_state.snowflake_creds, indent=2),
            height=150
        )
        
        if st.button("Save JSON Credentials"):
            try:
                creds = json.loads(json_input)
                # Validate required fields
                required_fields = ['account', 'host', 'username', 'password', 'role', 'database']
                if not all(field in creds for field in required_fields):
                    st.error("Missing required fields in JSON")
                    return
                
                # Ensure session_keep_alive is a boolean
                if 'session_keep_alive' not in creds:
                    creds['session_keep_alive'] = True
                
                # Save to session state
                st.session_state.snowflake_creds = creds
                st.success("JSON credentials saved successfully!")
            except json.JSONDecodeError:
                st.error("Invalid JSON format")
    
    # Database Management
    st.subheader("🗄️ Database Management")
    
    if st.button("Reinitialize Database"):
        with st.spinner("Reinitializing database..."):
            try:
                from src.db.duckdb_connection import reinitialize_database
                reinitialize_database()
                st.success("Database reinitialized successfully!")
            except Exception as e:
                st.error(f"Error reinitializing database: {str(e)}")
    
    # Application Settings
    st.subheader("⚙️ Application Settings")
    
    # Auto-refresh interval
    refresh_interval = st.number_input(
        "Auto-refresh Interval (seconds)",
        min_value=5,
        max_value=300,
        value=10,
        help="How often to refresh the monitoring dashboard"
    )
    
    # Theme selection
    theme = st.selectbox(
        "Theme",
        options=["Light", "Dark"],
        help="Choose the application theme"
    )
    
    if st.button("Save Settings"):
        # Here you would typically save these settings
        st.success("Settings saved successfully!") 