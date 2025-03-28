from sqlalchemy import create_engine, text

# Define your connection string.
conn_str = "oracle+oracledb://system:Oracle123@3.141.25.89:1521/?service_name=XEPDB1"
# Create the SQLAlchemy engine.
engine = create_engine(conn_str)

try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM XEPDB1.RETAIL_DEMO.CUSTOMERS;"))
        print("Connection successful! Test query result:", result.fetchone())
except Exception as e:
    print("Error connecting to the database:", e)
