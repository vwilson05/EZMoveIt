import duckdb

con = duckdb.connect('EZMoveIt.duckdb')

# Fetch the list of tables
tables = con.execute("SHOW TABLES;").fetchall()

# Loop through the tables and clear each one
for table in tables:
    table_name = table[0]
    con.execute(f"DELETE FROM {table_name};")
    con.commit()

print("All tables cleared!")
