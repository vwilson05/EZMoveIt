import streamlit as st
import pandas as pd
from snowflake.connector import connect

# Connect to Snowflake
def get_snowflake_connection():
    return connect(
        user="VICTOR_WILSON@HAKKODA.IO",
        account="XQEFMFM-HAKKODAINC_PARTNER",
        authenticator="externalbrowser",
        role="SYSADMIN",
        database="VICTOR_WILSON_SANDBOX",
        schema="DLT_TEST"
    )

# Run Query
def query_data_page(query):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    cur.close()
    conn.close()
    return df

# Streamlit UI
st.title("📊 Data Explorer - Snowflake")

query = st.text_area("📝 Enter SQL Query", "SELECT * FROM DLT_TEST.TODO LIMIT 10")

if st.button("Run Query"):
    df = run_query(query)
    st.dataframe(df)
