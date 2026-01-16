from snowflake.snowpark import Session

def create_snowflake_session():
    """
    Creates and returns a Snowflake session using predefined connection parameters.
    """
    connection_parameters = {
        "account": "ivoklic-wyb12201.US-WEST-OREGON.AWS",
        "user": "PranayIngle",
        "password": "Pranayi25#",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "SUSTAINABLITY_CORTEX_SEARCH_DOCS",
        "schema": "Data"
    }
    return Session.builder.configs(connection_parameters).create()