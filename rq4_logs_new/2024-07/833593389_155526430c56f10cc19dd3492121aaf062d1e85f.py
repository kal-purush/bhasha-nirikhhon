import duckdb

def read_from_duckdb_and_print(table_name: str, db_file: str = "my_duckdb.db"):
    """
    Read data from a DuckDB table and print the results.

    Parameters:
    - table_name: Name of the table from which the data will be read.
    - db_file: Path to the DuckDB file.
    """
    # Connect to DuckDB
    conn = duckdb.connect(database=db_file)

    # Execute SQL query
    query = f"SELECT * FROM {table_name}"
    result = conn.execute(query).fetchall()

    # Close the connection
    conn.close()

    # Print the results
    for row in result:
        print(row)


if __name__ == "__main__":
    # Name of the table for querying
    table_name = "kpi_table"

    # Read data from the table and print the results
    read_from_duckdb_and_print(table_name)