from sqlalchemy import create_engine, text

# Define your connection string.
conn_str = "mssql+pyodbc://sa:MyP%40ssw0rd123@3.141.25.89:1433/test_db?TrustServerCertificate=yes&driver=ODBC+Driver+17+for+SQL+Server"
# Create the SQLAlchemy engine.
engine = create_engine(conn_str)

try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM TEST_DB.RETAIL_DEMO.CUSTOMERS"))
        print("Connection successful! Test query result:", result.fetchone())
except Exception as e:
    print("Error connecting to the database:", e)
