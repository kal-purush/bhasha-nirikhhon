import mysql.connector

# Replace the placeholder values with your actual connection details
conn = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="0612049471!Aa",
    database="pokedex"
)

cursor = conn.cursor()

# Retrieve table names
cursor.execute("SHOW TABLES;")
tables = cursor.fetchall()

# Iterate over tables
for table in tables:
    table_name = table[0]
    
    # Retrieve column names
    cursor.execute(f"SHOW COLUMNS FROM {table_name};")
    columns = cursor.fetchall()
    column_names = ', '.join(column[0] for column in columns)
    
    # Retrieve data from the table
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()
    
    # Generate INSERT INTO statements for each row
    for row in rows:
        values = ', '.join(f"'{value}'" for value in row)
        insert_statement = f"INSERT INTO {table_name} ({column_names}) VALUES ({values});"
        print(insert_statement)  # Replace with writing to a file or executing the statement
    
conn.close()