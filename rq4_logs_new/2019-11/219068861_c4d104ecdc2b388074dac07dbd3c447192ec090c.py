'''This program asks the user for a range of addressids to search on
the Census Geocoder. For searched addresses, it saves the
match status and any successfully retrieved data in a pickle. At the 
end of the program, the pickle is exported to CSV. If the program
is terminated early, the pickle can be exported to CSV manually using 
the program pickle_to_csv.py'''

import urllib.request, urllib.parse, urllib.error
import ssl
import json
import numpy as np
import pandas as pd
import time
import pickle
from census_geocoding_functions import *


# ask user for addressid range to search in the current session
firstid = int(input('Enter first addressid to search: '))
lastid = int(input('Enter last addressid to search: '))


# set base parameters
returntype = 'geographies'
searchtype = 'address'
benchmark = 'Public_AR_Current'
vintage = 'Current_Current'
dataformat = 'json'
layers = ''
baseurl = 'https://geocoding.geo.census.gov/geocoder/' + returntype + '/' + searchtype + '?'
parameters = {'benchmark': benchmark, 'vintage': vintage, 'format': dataformat}
# This search uses the most current benchmark and most current vintage of geographic
# information from the Census Geocoder.


# load in address data and set search index
print('\nReading in addresses...', end = ' ')
df = pd.read_pickle('addresses.pkl')
print('Done')

searchindex = df[(df['addressid'] >= firstid) & (df['addressid'] <= lastid)].index
numaddresses = len(searchindex)
if numaddresses == 0:
    print('No addresses in search range')
    quit()


# retrieve geographic data, set match status, add data to dataframe, and pickle 
# the dataframe after each search
print('\nRetrieving data...')
searchcount = 0
searchstart = time.time()
reportratefreq = 25
reportratestart = time.time()
for i in searchindex:
    searchcount += 1
    print('Address', searchcount, 'of', str(numaddresses) + ':', end = ' ')
    
    connection = open_connection(df, i, baseurl, parameters)
    if connection == None:
        matchstatus = 'Connection Error'
    else:
        geodata = retrieve_geodata(connection)
        if geodata == None:
            matchstatus = 'Parsing Error'
        elif len(geodata) == 0:
            matchstatus = 'No Match'
        else:
            matchstatus = 'Match'
            for var in ['matchedaddress','longitude','latitude',
                        'countycode','censustractcode']:
                df.loc[i,var] = geodata[var] 
            
    print(matchstatus)
    df.loc[i,'matchstatus'] = matchstatus
    
    df.to_pickle('addresses.pkl')
    
    if searchcount % reportratefreq == 0:
        reportratestop = time.time()
        print('(Current address retrieval/save rate:', round((reportratestop - reportratestart)/reportratefreq, 2), 'seconds per address)')
        reportratestart = reportratestop


# report search statistics
searchstop = time.time()
print('Took', round(searchstop - searchstart, 2), 'seconds to retrieve data on', numaddresses, 'addresses')
print('(' + str(round((searchstop - searchstart) / numaddresses, 2)), 'seconds per address)')

tabmatchstatus = df['matchstatus'][searchindex].value_counts()
print('\n-----Match Success Report-----')
for status in tabmatchstatus.index:
    print(status + ':', tabmatchstatus[status], 'addresses', '(' + str(round(tabmatchstatus[status]/numaddresses*100,2)) + '%)')


# export data to CSV file
print('\nSaving data...')
savestart = time.time()
df.to_csv('geocoded-addresses.csv', index = False)
savestop = time.time()
print('Took', round(savestop - savestart, 2), 'seconds to save data with', df.shape[0], 'observations')