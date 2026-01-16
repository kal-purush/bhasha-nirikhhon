import os
from datetime import datetime
import random

from pymongo import MongoClient
from dotenv import load_dotenv

from location import buildLocation


load_dotenv()


MONGODB_CLIENT=str(os.getenv("MONGODB_CLIENT"))
DEVICE_LABEL = os.getenv('DEVICE_LABEL')
DATABASE_NAME = os.getenv('DATABASE_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')

VARIABLE_LABEL_1 = os.getenv('VARIABLE_LABEL_1')

def getTime():
    '''
    Get date and time
    :return: {date: x, clock: x}
    '''
    now= datetime.now()
    jam = now.strftime("%H:%M:%S")
    tanggal = now.strftime(f'%B %d, %Y')

    dataTime = {
                'date'  : tanggal,
                'time'  : jam
    }
    
    return dataTime

def randomData():
    '''
    Generate random data for temperature, spo2, hearth rate
    '''
    temp = round(random.uniform(35,40), 1)
    spo2 = random.randint(90,100)
    hr = random.randint(60,100)
    return temp, spo2, hr

def initializeDatabase():
    client = MongoClient(MONGODB_CLIENT)
    db = client[DATABASE_NAME]
    my_collections = db[COLLECTION_NAME]

    return my_collections

def buildData(id, temperature, spo2, hearth_rate, timestamp, location):
    '''
    Accept arguments: id, temperature, spo2, hearth_rate, timestamp, location\n
    timestamp dict {date, time}, location {latitude, longitude}
    '''
    data = {
                'id'                : id,
                'temperature'       : temperature,
                'spo2'              : spo2,
                'hearth rate'       : hearth_rate,
                'timestamp'         : timestamp,
                'location'          : location
    }
    
    return data

def sendDataMongoDB(data):
    '''
    Insert one data to MongoDB
    '''
    my_collections = initializeDatabase()

    results = my_collections.insert_one(data)

    return results

def getLatestData():
    '''
    Get latest data from MongoDB
    Use for function to print all result
    '''
    my_collections = initializeDatabase()

    result = my_collections.find().sort('id', -1).limit(1)

    try:
        a = result[0]

    except IndexError:
        return None

    return result[0]

def deleteData(criteria):
    '''
    Delete data with given criteria
    :accept: dict {'key': 'value'}
    '''
    my_collections = initializeDatabase()

    results = my_collections.delete_one(criteria)

    return results

if __name__ == '__main__':
    dataRandom = randomData()
    location = buildLocation()
    timestamp = getTime()

    latest = getLatestData()
    if latest is None:
        data = buildData(1, dataRandom[0], dataRandom[1], dataRandom[2], timestamp, location)

    else:
        idNew = latest['id'] + 1
        data = buildData(idNew, dataRandom[0], dataRandom[1], dataRandom[2], timestamp, location)

    try:
        sendDataMongoDB(data)

    except:
        print('[INFO] Error, cannot send data to MongiDB')

    print('[INFO] Data sent!')
    print(f'{data}')