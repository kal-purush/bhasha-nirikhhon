import pandas as pd

def cleaningData():

    rawUMoron = pd.read_csv('/home/lautaror/airflow/dags/files/queryUMoron.csv')
    rawUNRC = pd.read_csv('/home/lautaror/airflow/dags/files/queryUNRC.csv')
    postalCodes = pd.read_csv('/home/lautaror/airflow/dags/files/codigos_postales.csv')

    comma= '-'
    space= ' '
    exchange=str.maketrans(comma, space)

    postalCodes = postalCodes.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'})
    postalCodes['postal_code'] = postalCodes['postal_code'].astype('str')
    postalCodes['location'] = postalCodes['location'].str.lower().str.translate(exchange).str.strip()

    rawUMoron['university'] = rawUMoron['university'].str.lower().str.translate(exchange).str.strip()
    rawUMoron['career'] = rawUMoron['career'].str.lower().str.translate(exchange).str.strip()
    rawUMoron['first_name'] = rawUMoron['first_name'].str.lower().str.translate(exchange).str.strip()
    rawUMoron['last_name'] = rawUMoron['last_name'].str.lower().str.translate(exchange).str.strip()
    rawUMoron['email'] = rawUMoron['email'].str.lower().str.translate(exchange).str.strip()
    rawUMoron['gender'] = rawUMoron['gender'].replace('M', 'male').replace('F', 'female')
    rawUMoron['inscription_date'] = pd.to_datetime(rawUMoron['inscription_date'], format='%Y-%m-%d')
    rawUMoron['postal_code'] = rawUMoron['postal_code'].astype('str')
    dfUMoron = pd.merge(rawUMoron, postalCodes, on='postal_code')
    dfUMoron.to_csv('/home/lautaror/airflow/dags/files/dataUMoron.txt', index=False)

    rawUNRC['university'] = rawUNRC['university'].str.lower().str.translate(exchange).str.strip()
    rawUNRC['career'] = rawUNRC['career'].str.lower().str.translate(exchange).str.strip()
    rawUNRC['first_name'] = rawUNRC['first_name'].str.lower().str.translate(exchange).str.strip()
    rawUNRC['last_name'] = rawUNRC['last_name'].str.lower().str.translate(exchange).str.strip()
    rawUNRC['email'] = rawUNRC['email'].str.lower().str.translate(exchange).str.strip()
    rawUNRC['gender'] = rawUNRC['gender'].replace('M', 'male').replace('F', 'female')
    rawUNRC['inscription_date'] = pd.to_datetime(rawUNRC['inscription_date'], format='%Y-%m-%d')
    rawUNRC['location'] = rawUNRC['location'].str.lower().str.translate(exchange).str.strip()
    dfUNRC = pd.merge(rawUNRC, postalCodes, on='location')
    dfUNRC.to_csv('/home/lautaror/airflow/dags/files/dataUNRC.txt', index=False)
