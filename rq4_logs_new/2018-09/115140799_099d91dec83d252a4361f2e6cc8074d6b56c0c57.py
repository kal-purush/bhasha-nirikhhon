
def validate_column(target_column, target_table, cursor):
    """ Makes sure column belongs to table

    Args:
         target_column (str): column in question
         target_table (str): table name
         cursor (pyodbc.Cursor): active cursor

    Returns:
        validated column name

    Raises:
         ValueError if target_column is not valid in target_table
    """
    valid_columns = []
    for column in cursor.columns(target_table):
        valid_columns.append(column.column_name)
    if target_column not in valid_columns:
        cursor.close()
        raise ValueError(
            '\"{}\" is not a valid column in table \"{}\".'.format(
                target_column, target_table))
    return target_column