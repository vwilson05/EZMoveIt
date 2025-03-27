#!/usr/bin/env python3
import sys
import random
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime

# Usage: python3 insert_rows.py [total_rows] [batch_size]
total_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

# Replace with your connection string.
conn_str = "mssql+pyodbc://sa:MyP%40ssw0rd123@3.141.25.89:1433/test_db?TrustServerCertificate=yes&driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str)
metadata = MetaData()

# Define your customers table with explicit schema (adjust if needed).
customers = Table("customers", metadata,
    Column("CustomerID", Integer, primary_key=True),
    Column("Firstname", String(50)),
    Column("LastName", String(50)),
    Column("Email", String(100)),
    Column("Phone", String(20)),
    Column("CreatedAt", DateTime),
    schema="retail_demo"  # Adjust if your table is in a different schema.
)

# Uncomment the following line if the table does not exist:
# metadata.create_all(engine)

# Prepare sample data lists.
first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Wilson", "Moore"]

def random_email(first, last):
    return f"{first.lower()}.{last.lower()}{random.randint(1,1000)}@example.com"

def random_phone():
    return str(random.randint(1000000000, 9999999999))

# Generate the rows.
rows = []
for i in range(total_rows):
    first = random.choice(first_names)
    last = random.choice(last_names)
    row = {
        "Firstname": first,
        "LastName": last,
        "Email": random_email(first, last),
        "Phone": random_phone(),
        "Created_at": datetime.datetime.now()
    }
    rows.append(row)

print(f"Total rows prepared: {len(rows)}")
print(f"Starting batch insert with batch size: {batch_size}")

# Insert in batches.
for i in range(0, total_rows, batch_size):
    batch = rows[i:i+batch_size]
    with engine.begin() as conn:
        conn.execute(customers.insert(), batch)
    print(f"Inserted rows {i+1} to {i + len(batch)}")

print("Insert complete.")
