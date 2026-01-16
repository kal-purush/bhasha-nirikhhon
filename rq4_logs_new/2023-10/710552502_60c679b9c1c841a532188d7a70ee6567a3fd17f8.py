import re
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

DATABASE_NAME = "covid_db"
DB_PARAMS = {
    "host": "localhost",
    "user": "postgres",
    "password": "Vanadio23"
}


def main():
    """
    Calls all functions relevant to perform database creation and first load of the datasets.
    """

    create_db()
    create_schema('covid_data')
    create_schema('country_data')

    transformed_covid_df, transformed_country_df = execute_extract_transform()

    covid_table_params = {"schema_name": "covid_data",
                          "table_name": "national_14day_notification_rate_covid_19",
                          "primary_key_cols": ["country", "year_week", "indicator"]}

    country_table_params = {
        "schema_name": "country_data",
        "table_name": "countries_of_the_world",
        "primary_key_cols": ["country"]
    }

    create_table(transformed_covid_df, covid_table_params)
    create_table(transformed_country_df, country_table_params)

    insert_dataframe_to_postgres(
        transformed_covid_df, covid_table_params['table_name'], covid_table_params['schema_name'])
    insert_dataframe_to_postgres(
        transformed_country_df, country_table_params['table_name'], country_table_params['schema_name'])


def get_national_14day_covid_data() -> pd.DataFrame:
    """
    This function retrieves the JSON data from the provided datasource.

    Returns:
        df_covid: pd.DataFrame with data on 14-day notification rate of new COVID-19 cases and deaths.
    """
    try:
        response = requests.get(
            "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/")
        data = response.json()
        df_covid = pd.DataFrame(data)

    except:
        raise

    return df_covid


def extract_phase() -> pd.DataFrame:
    """
    This function executes de Extraction phase for the first run of the ETL.

    Returns:
        df_covid_data: pd.DataFrame with data on 14-day notification rate of new COVID-19 cases and deaths and deaths.
        df_country_data: pd.DataFrame with socioeconomic data on countries of the world.
    """
    df_covid_data = get_national_14day_covid_data()
    df_country_data = pd.read_csv('countries_of_the_world.csv')

    return df_covid_data, df_country_data


def correct_column_name(name: str) -> str:
    """
    This function standardize column names for dataframes, removing spaces and special characters and converting every upper to lower case. 

    Args: 
        - name: string with the name of the column to be treated.

    Returns:
        str: string with corrected column name.
    """
    name = re.sub(r'[^\w\s]', '', name)
    name = name.lower().strip()
    name = name.replace(" ", "_")

    return name


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function corects column names through iterating a loop on every column, calling the standardize_column_name function. 

    Args: 
        - df: pd.Dataframe with the dataframe to be treated.

    Returns:
        pd.DataFrame: Pandas DataFrame with corrected column names.
    """

    df.columns = [correct_column_name(column) for column in df.columns]

    return df


def convert_string_to_float_columns(list_of_columns: list, df: pd.DataFrame) -> pd.DataFrame:
    """
    This function corrects column types for dataframes with float numbers incorrectly using the ',' character instead of '.' to express numeric precision. It converts them from string to float by correcting the character.

    Args: 
        - list_of_columns: list of strings with the names of columns to be treated.
        - df: pd.Dataframe to be treated.

    Returns:
        df: Pandas DataFrame with corrected column types.
    """

    for column in list_of_columns:
        df[column] = df[column].str.replace(',', '.').astype(float)

    return df


def add_updated_at_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds column with 'updated_at' column that will be necessary for scheduling the incremental loads.

    Args: 
        - df: dataframe to receive the column.

    Returns:
        - df: dataframe with new column.
    """
    current_datetime = datetime.now().date()

    df['updated_at'] = current_datetime

    return df


def transform_phase(df: pd.DataFrame, string_to_float_columns_list=None) -> pd.DataFrame:
    """
    This function executes all relevant transformation to the dataframes prior to the initial load.

    Args: 
        - df: pd.Dataframe to be treated.
        - string_to_float_columns_list: list of strings with the names of columns to be treated from string to float types. Standard value is None, in case there are no columns with string to float conversions to be made.

    Returns:
       transformed_df: Pandas DataFrame with final transformations.
    """

    transformed_df = standardize_column_names(df)

    if string_to_float_columns_list:
        transformed_df = convert_string_to_float_columns(
            string_to_float_columns_list, transformed_df)

    transformed_updated_df = add_updated_at_column(transformed_df)

    return transformed_updated_df


def create_sql_script(df:  pd.DataFrame, table_name: str, schema_name: str, primary_key_cols=None) -> str:
    """
    This function creates the script that will be used to create the tables in the database prior to the first load, based on its column types. A primary key for each table can also be defined in this script. 

    Args: 
        - df: dataframe to be inserted into table.
        - table_name: name that the table will have in the PostgreSQL database.
        - schema_name: name of the schema where the table will be set.
        - primary_key_cols: list of column names to be used as table primary key. Can be a list containing a single value.Will be None in case a primary key is not to be set. 

    Returns:
       sql_script: the string containing the sql script with column names and types to create new tables. 
    """

    data_types = {
        "int64": "INTEGER",
        "float64": "NUMERIC",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN"
    }

    # Initializes the SQL script with the CREATE TABLE command
    sql_script = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"

    # Loops through the DataFrame columns
    for column in df.columns:
        # Use TEXT as the default type
        data_type = data_types.get(str(df[column].dtype), "TEXT")
        nullability = "NOT NULL" if df[column].notnull().all() else "NULL"
        sql_script += f"    {column} {data_type} {nullability},\n"

    # Adds the primary key declaration if columns are specified
    if primary_key_cols:
        primary_key = ", ".join(primary_key_cols)
        sql_script += f"    PRIMARY KEY ({primary_key}),\n"

    # Removes the extra comma at the end and close the CREATE TABLE command
    sql_script = sql_script[:-2] + "\n);"

    return sql_script


def connect_to_postgres(database=None) -> psycopg2.connect:
    """
    This function creates a connection to local PostgreSQL database.

    Args: 
        - database: string containaing the name of the database to be connected with. Standard value is None in case the - - connection is not to be made to an specific database, e.g. when creating a new database.

    Returns:
       conn: psycopg2.connection object that contains connection to database.
    """
    if database:
        conn = psycopg2.connect(database=database, **DB_PARAMS)
    else:
        conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    return conn


def execute_create_sql_command(object_name: str, object_type: str, schema_name=None, create_table_sql=None) -> None:
    """
    This function creates the sql command that will be used to create either a database, schema or table within our PostgreSQL database. It utilizes our connection to PostgreSQL to create the desired object. 

    Args:
        - object_name: string containing the name o the object to be created.
        - object_type: string containing the type of the object to be created. Accepted values are 'database', 'schema' and 'table'.
        - schema_name: optional value. String containing name of the schema where a table is created.
        - create_table_sql: optional value. String containing the SQL script with the CREATE TABLE command for a given table.     
    """

    try:
        if object_type == 'database':
            conn = connect_to_postgres()
            cursor = conn.cursor()
            cursor.execute(f"DROP {object_type} IF EXISTS {object_name};")
            # Execute the SQL command to create the object
            cursor.execute(f"CREATE {object_type} {object_name};")
            print(f"{object_type} '{object_name}' created successfully!")

        elif object_type == 'schema':
            conn = connect_to_postgres(database=DATABASE_NAME)
            cursor = conn.cursor()
            cursor.execute(
                f"DROP {object_type} IF EXISTS {object_name} CASCADE;")
            cursor.execute(
                f"CREATE {object_type} IF NOT EXISTS {object_name};")
            print(f"{object_type} '{object_name}' created successfully!")

        elif object_type == 'table':
            conn = connect_to_postgres(database=DATABASE_NAME)
            cursor = conn.cursor()
            cursor.execute(
                f"DROP {object_type} IF EXISTS {schema_name}.{object_name};")
            cursor.execute(create_table_sql)
            print(
                f"{object_type} '{schema_name}.{object_name}' created successfully!")

    except (Exception, psycopg2.Error) as error:
        raise error

    finally:
        if conn:
            conn.close()


def insert_dataframe_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str, if_exists='replace') -> None:
    """
    Inserts the treated dataframe into the tables created in PostgreSQL database.

    Args: 
        - df: pd.DataFrame to be inserted.
        - table_name: name of the table in PostgreSQL database
        - schema_name: name of schema containing table in PostgreSQL database
        - if_exists: specifies the behavior if the table already exists. This script is supposed to make one batch ingestion with all existing data on the data sources given, so we choose to replace. Avoid this method for incremental loads. 
    """
    engine = create_engine(
        f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}/{DATABASE_NAME}')

    try:
        df.to_sql(table_name, schema=schema_name, con=engine,
                  if_exists=if_exists, index=False)
    except SQLAlchemyError as e:
        raise e


def create_db() -> None:
    """
    Creates our database.
    """
    params = {
        "object_name": DATABASE_NAME,
        "object_type": "database"
    }
    execute_create_sql_command(**params)


def create_schema(schema_name: str) -> None:
    """
    Creates the schemas for our datasets.
    Args:
        - schema_name: name of the schema we want to create for our datasets.
    """

    params = {
        "object_name": schema_name,
        "object_type": "schema"
    }

    execute_create_sql_command(**params)


def execute_extract_transform() -> pd.DataFrame:
    """
    Executes every transformation relevant to the datasets by calling other functions.

    Returns:
        - transformed_covid_data: trasnformed dataset with covid cases and death information.
        - transformed_country_data: transformed dataset with country information.
    """

    df_covid_data, df_country_data = extract_phase()

    string_to_float_columns_country_data = ["pop_density_per_sq_mi", "coastline_coastarea_ratio", "net_migration", "infant_mortality_per_1000_births",
                                            "literacy", "phones_per_1000", "arable", "crops", "other", "birthrate", "deathrate", "agriculture", "industry", "service"]

    transformed_covid_data = transform_phase(df_covid_data)
    transformed_country_data = transform_phase(
        df_country_data, string_to_float_columns_country_data)

    return transformed_covid_data, transformed_country_data


def create_table(df: pd.DataFrame, table_params: dict):
    """
    Calls function to create SQL cript with CREATE TABLE command and executes it in order to create tables.

    Args:
        - df: dataframe that originates the table.
        - table_params: parameters necessary for creating the script. 
    """

    script_table = create_sql_script(df, **table_params)

    sql_params = {
        "object_name": table_params['table_name'],
        "object_type": "table",
        "schema_name": table_params['schema_name'],
        "create_table_sql": script_table
    }

    execute_create_sql_command(**sql_params)


if __name__ == "__main__":
    main()