#!/usr/bin/env python3
import sys
import random
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Numeric

# Usage: python3 insert_rows.py [total_rows] [batch_size]
total_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1000000
batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10000

# Connection string for SQL Server
conn_str = "mssql+pyodbc://sa:MyP%40ssw0rd123@3.141.25.89:1433/test_db?TrustServerCertificate=yes&driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str)
metadata = MetaData()

# # Define tables in schema "retail_demo"
# customers = Table("customers", metadata,
#     Column("CustomerID", Integer, primary_key=True, autoincrement=True),
#     Column("Firstname", String(50)),
#     Column("LastName", String(50)),
#     Column("Email", String(100)),
#     Column("Phone", String(20)),
#     Column("CreatedAt", DateTime),
#     Column("UPDATED_DT", DateTime),
#     schema="retail_demo"
# )

# stores = Table("Stores", metadata,
#     Column("StoreID", Integer, primary_key=True, autoincrement=True),
#     Column("StoreName", String(100)),
#     Column("City", String(50)),
#     Column("State", String(50)),
#     schema="retail_demo"
# )

products = Table("Products", metadata,
    Column("ProductID", Integer, primary_key=True, autoincrement=True),
    Column("ProductName", String(100)),
    Column("Category", String(50)),
    Column("Price", Numeric(10,2)),
    Column("Stock", Integer),
    schema="retail_demo"
)

orders = Table("Orders", metadata,
    Column("OrderID", Integer, primary_key=True, autoincrement=True),
    Column("CustomerID", Integer),
    Column("StoreID", Integer),
    Column("OrderDate", DateTime, default=datetime.datetime.now),
    schema="retail_demo"
)

orderitems = Table("OrderItems", metadata,
    Column("OrderItemID", Integer, primary_key=True, autoincrement=True),
    Column("OrderID", Integer),
    Column("ProductID", Integer),
    Column("Quantity", Integer),
    Column("UnitPrice", Numeric(10,2)),
    schema="retail_demo"
)

# Uncomment if tables are not already created:
# metadata.create_all(engine)

# --- Data Generation Functions ---

# Customers
first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Wilson", "Moore"]

def random_email(first, last):
    return f"{first.lower()}.{last.lower()}{random.randint(1,1000)}@example.com"

def random_phone():
    return str(random.randint(1000000000, 9999999999))

# customer_rows = []
# for i in range(total_rows):
#     first = random.choice(first_names)
#     last = random.choice(last_names)
#     row = {
#         "Firstname": first,
#         "LastName": last,
#         "Email": random_email(first, last),
#         "Phone": random_phone(),
#         "CreatedAt": datetime.datetime.now(),
#         "UPDATED_DT": datetime.datetime.now()
#     }
#     customer_rows.append(row)

# # Stores
# store_names = ["Store Alpha", "Store Beta", "Store Gamma", "Store Delta", "Store Epsilon"]
# cities = ["CityX", "CityY", "CityZ"]
# states = ["State1", "State2", "State3"]
# stores_rows = []
# for i in range(total_rows):
#     row = {
#         "StoreName": random.choice(store_names) + " " + str(random.randint(1,100)),
#         "City": random.choice(cities),
#         "State": random.choice(states)
#     }
#     stores_rows.append(row)

# Products
product_names = ["Widget", "Gadget", "Thingamajig", "Doodad", "Doohickey"]
categories = ["Category A", "Category B", "Category C"]
products_rows = []
for i in range(total_rows):
    row = {
        "ProductName": random.choice(product_names) + " " + str(random.randint(1,1000)),
        "Category": random.choice(categories),
        "Price": round(random.uniform(5.0, 500.0), 2),
        "Stock": random.randint(0, 1000)
    }
    products_rows.append(row)
    
# Orders
orders_rows = []
for i in range(total_rows):
    row = {
        "CustomerID": random.randint(1, total_rows),
        "StoreID": random.randint(1, total_rows),
        "OrderDate": datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))
    }
    orders_rows.append(row)

# OrderItems
orderitems_rows = []
for i in range(total_rows):
    row = {
        "OrderID": random.randint(1, total_rows),     # Assuming orders are sequential from 1 to total_rows
        "ProductID": random.randint(1, total_rows),     # Assuming products are sequential from 1 to total_rows
        "Quantity": random.randint(1, 10),
        "UnitPrice": round(random.uniform(1.0, 100.0), 2)
    }
    orderitems_rows.append(row)

# print(f"Total customers prepared: {len(customer_rows)}")
# print(f"Total stores prepared: {len(stores_rows)}")
print(f"Total products prepared: {len(products_rows)}")
print(f"Total orders prepared: {len(orders_rows)}")
print(f"Total orderitems prepared: {len(orderitems_rows)}")
print(f"Starting batch insert with batch size: {batch_size}")

def insert_in_batches(table, rows):
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        with engine.begin() as conn:
            conn.execute(table.insert(), batch)
        print(f"Inserted rows {i+1} to {i+len(batch)} into {table.name}")

# # --- Insertion Order: Customers, Stores, Orders, Products, OrderItems ---
# print("Inserting Customers...")
# insert_in_batches(customers, customer_rows)

# print("Inserting Stores...")
# insert_in_batches(stores, stores_rows)

print("Inserting Products...")
insert_in_batches(products, products_rows)

print("Inserting Orders...")
insert_in_batches(orders, orders_rows)

print("Inserting OrderItems...")
insert_in_batches(orderitems, orderitems_rows)

print("Insert complete.")
