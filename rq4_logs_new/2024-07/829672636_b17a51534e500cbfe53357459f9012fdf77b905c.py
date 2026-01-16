import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError

# Define your MySQL connection parameters
user = 'root'
password = 'rootpassword'
host = 'localhost'
port = 3306
database = 'bosta_asses'

# Create SQLAlchemy engine
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
db_engine = create_engine(connection_string)

# Fetch data from the API
response = requests.get("https://randomuser.me/api/")
result = response.json()['results'][0]

# Create DataFrames from the fetched data
user_df = pd.DataFrame([{
    'gender': result['gender'],
    'title': result['name']['title'],
    'first_name': result['name']['first'],
    'last_name': result['name']['last'],
    'email': result['email'],
    'user_id': result['login']['uuid'],
    'username': result['login']['username'],
    'password': result['login']['password'],
    'salt': result['login']['salt'],
    'md5': result['login']['md5'],
    'sha1': result['login']['sha1'],
    'sha256': result['login']['sha256'],
    'dob': pd.to_datetime(result['dob']['date']).strftime('%Y-%m-%d %H:%M:%S'),
    'dob_age': result['dob']['age'],
    'registered': pd.to_datetime(result['registered']['date']).strftime('%Y-%m-%d %H:%M:%S'),
    'registered_age': result['registered']['age'],
    'phone': result['phone'],
    'cell': result['cell'],
    'id_name': result['id']['name'],
    'id_value': result['id']['value'],
    'nat': result['nat']
}])

location_df = pd.DataFrame([{
    'street_number': result['location']['street']['number'],
    'street_name': result['location']['street']['name'],
    'city': result['location']['city'],
    'state': result['location']['state'],
    'country': result['location']['country'],
    'postcode': str(result['location']['postcode']).replace(' ', ''),
    'latitude': result['location']['coordinates']['latitude'],
    'longitude': result['location']['coordinates']['longitude'],
    'timezone_offset': result['location']['timezone']['offset'],
    'timezone_description': result['location']['timezone']['description'],
    'user_id': result['login']['uuid']
}])

pictures_df = pd.DataFrame([{
    'large_url': result['picture']['large'],
    'medium_url': result['picture']['medium'],
    'thumbnail_url': result['picture']['thumbnail'],
    'user_id': result['login']['uuid']
}])

# Define the tables metadata
metadata = MetaData(bind=db_engine)
users_table = Table('users', metadata, autoload=True)
locations_table = Table('locations', metadata, autoload=True)
pictures_table = Table('pictures', metadata, autoload=True)

# Insert data into the database
all_successful = True

def insert_data(table, data):
    global all_successful
    try:
        with db_engine.connect() as conn:
            conn.execute(table.insert().values(data.to_dict(orient='records')))
    except SQLAlchemyError as e:
        print(f"Error inserting data into the '{table.name}' table: {e}")
        all_successful = False

insert_data(users_table, user_df)
insert_data(locations_table, location_df)
insert_data(pictures_table, pictures_df)

if all_successful:
    print("One record has been inserted successfully.")