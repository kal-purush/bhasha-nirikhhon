import os
# Configure the connection parameters as needed
default_user_name       = "root"
default_password        = ""
default_host            = "localhost"
default_port            = "3306"
# Configure mySQL database and table names
target_database = 'swiss_banking'
table_name      = "currency_exchange_rates"
# Configure the URL for the CSV data
csv_url         = "https://data.snb.ch/api/cube/devkua/data/csv/en"

# SQLite database url
script_directory = os.path.dirname(os.path.abspath(__file__))
sqlite_database_url = f"sqlite:///{os.path.join(script_directory, f'{target_database}.db')}"