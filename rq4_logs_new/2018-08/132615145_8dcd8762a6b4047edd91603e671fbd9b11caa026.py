import requests
import json
import keyring
import snowflake.connector
from datetime import datetime, timedelta
import os
from difflib import SequenceMatcher
import logging

# function to compare two strings through pattern matching
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# this creates a log file for the script

logging.basicConfig(

    filename='C:/python36/scripts/logs/snowflake_python_connector.log',
    level=logging.INFO)

USER = 'PYTHON_SERVICE'
PASSWORD = keyring.get_password("Snowflake", "PYTHON_SERVICE")

ACCOUNT = 'lillypulitzer'
WAREHOUSE = 'STITCH_TEST'
DATABASE = 'staging'
SCHEMA = 'Python_production'

# euclid api token
token='f4c42670-ece4-47ee-b13f-e849e45dd701'
# limit the euclid api to 3000 entries max
limit = 3000

os.environ['TZ']='GMT'
pattern = '%Y-%m-%d %H:%M:%S.%f'
# set days to how many days in the past to retrieve. 1 day is the default (yesterday)
days = 1

# open connection to Snowflake instance
cnx = snowflake.connector.connect(user=USER,
                                  password=PASSWORD,
                                  account=ACCOUNT,
                                  warehouse=WAREHOUSE,
                                  database=DATABASE,
                                  schema=SCHEMA)

# read stores into an array
sql = "SELECT * FROM STAGING.DB2_PROD2.STORENORMALIZED where district_name <> 'ECOMMERCE' AND " \
      "district_name <> 'DISTRIBUTION CENTER' order by store_name"
cursor_list = cnx.execute_string(sql)

for cursor in cursor_list:
    storeArray = list(cursor)

# main processing
# set the target date, delete any rows for that date just in case the job is being re-run
# get today's date, but subtract one day for yesterday

for days in range(1,2):
    today = (datetime.today() - timedelta(days=(days))).strftime('%Y-%m-%d')

    print ("**** " + today + " ****")
    # delete any existing rows from a previous run
    # this allows the job to be re-run if it fails for any reason
    sql = "DELETE FROM STAGING.EUCLID.VISITORS WHERE LAST_VISIT_DATE = %s"
    args = str(today)
    cnx.cursor().execute(sql, args)

    # Get all Euclid visit via their API
    # Provide yesterday's date for both start and end
    # We are only fetching a single days worth
    response_native = ""
    r = requests.get('https://metrics-api.euclidelements.com/v1/segmentation/session-details/preview?hierarchy-path=All&euclid-enabled-only=true&token='
                      + token + '&earliest-date=' + str(today) + '&latest-date=' + str(today) + '&columns=visit-date%2Cemail-address%2Cage%2Clast-visit-date%2Cvisit-location%2Clast-visit-time-of-day%2Clikelihood-of-visit-next-30-days%2Cvisitor-id%2Cduration%2Cage%2Cvisitor-id&limit=' + str(limit))
    response_native = json.loads(r.text)
    insert_counter = 0
    insert_array = []

    # process each json entry
    for each in response_native:
        insert_counter +=1
        email_address = each['email_address']
        duration = str(each['duration'])
        age = each['age']
        last_visit = each['visit_date']
        last_visit_time = each['last_visit_time_of_day']
        likelihood = each['likelihood_of_visit_next_30_days']
        visitor_id = each['visitor_id']
        store_name = each['visit_location']

    # iterate through stores, matching euclid store name to Lilly master store name
    # Established similarity metric of .55 or higher to match the store

        for store in storeArray:
            cleanStore = store[1].upper().strip()
            cleanEuclidStore = each['visit_location'].upper().strip()

            cleanStore = cleanStore.replace('TOWN CENTER', '')
            cleanEuclidStore = cleanEuclidStore.replace('TOWN CENTER', '')

            if similar(cleanStore, cleanEuclidStore) >= .55:
                print("match : " + cleanEuclidStore)
                visit_location = store[0]
                break

    # create array of 500 inserts
        if insert_counter < 500:
            insert_array.append([email_address, age, last_visit, last_visit_time, likelihood, visit_location, visitor_id, duration, store_name])
        else:
            sql = "INSERT INTO STAGING.EUCLID.VISITORS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            args = insert_array
            cnx.cursor().executemany(sql, args)
            del insert_array[:]
            insert_counter = 0

    # finished processing, grab last group of records and insert. close the connection to Snowflake
    if insert_counter > 0:
        sql = "INSERT INTO STAGING.EUCLID.VISITORS VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        args = insert_array
        cnx.cursor().executemany(sql, args)
        del insert_array[:]
        insert_counter = 0

cnx.close()