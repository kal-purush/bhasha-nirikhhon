
####
#import libraries
####

#webservices and apis
import json
import urllib2

#generic libs
import csv
from time import time,sleep
import logging


###
#now setup config options
###

def getAPIKey(key_file="api_key"):
    f = open(key_file,'r')    
    api_key = f.readline()
    logging.info( str(time())+" API Key "+api_key)
    return api_key

key =getAPIKey()
print key
contract = "Dublin"

outputfolder = "/home/pi/Python/Bikes/"
outputdoc = "data.csv"
outputfile = outputfolder+outputdoc

delay = 30 #how many seconds do you want to wait before polling again
errdelay = 0.25 #how many seconds to wait to repoll after error
max_attempts = 10 # max consecutive attempts before quit

#https://docs.python.org/2/howto/logging.html
logging.basicConfig(filename='errors.log',level=logging.WARNING) #WARNING

###
#define functions
###



def getStaticData(contract):
    #get static data - may want to use this for iterating or as a reference table
    url = "https://developer.jcdecaux.com/rest/vls/stations/"+contract+".json"
    try:
        response = urllib2.urlopen(url).read()
    except:
        print "Error in getSaticData opening "+url
    return response

def getAllStationsData(api_key,contract):
    logging.debug( str(time())+"In getallstationdata")
    url = "https://api.jcdecaux.com/vls/v1/stations?contract="
    url = url+contract+"&apiKey="+api_key
    try:
        response = urllib2.urlopen(url).read()       
    except urllib2.HTTPError,e:
        logging.error( str(time())+" HTTP Error -"+str(e))
    except urllib2.URLError,e:
        logging.debug( str(time())+"in url error")
        logging.debug( str(time())+str(e))
        logging.error(str(time())+" URL Error -"+str(e))
    except urllib2.HTTPException,e:
        logging.error( str(time())+" HTTP Exception ")
    except Exception:
        logging.error( str(time())+" Error in getAllStationsData opening "+url)
    return response

def getStationData(api_key,contract,station_num):
    #test is input an int - to dev - if not int then lookup the station number by name
    logging.debug( str(time())+"In getstationdata")
    station_num = str(station_num) #convert to string. should also check this and trim it?
    url = "https://api.jcdecaux.com/vls/v1/stations/"+station_num+"?contract="
    url = url+contract+"&apiKey="+api_key
    #print url
    try:
        response = urllib2.urlopen(url).read()
    except:
        print "Error in getStationData opening "+url
    return response

def parseStationData(str_data,*args):
    #function to create the result object
    #by default this will just get station number, contract and timestamp
    #additional arguments are passed by args
    obj_data = json.loads(str_data)
    return_data = {}
    return_data['number'] = obj_data['number']
    return_data['contract_name'] = obj_data['contract_name']
    return_data['last_update'] = obj_data['last_update']

    #get optional arguments
    for a in args:
        #print a
        return_data[a] = obj_data[a] #add a test here
    return return_data

def parseAllStationData(str_data,*args):
    #not working... need to fix the iteration***
    #more efficient to work with this than iterating over list of stations
    #function to create the result object
    #by default this will just get station number, contract and timestamp
    #additional arguments are passed by args
    obj_data = json.loads(str_data)
    return_data = {}
    i=0
    for o in obj_data:
        return_data[i] = {}
        return_data[i]['number'] = o['number']
        return_data[i]['contract_name'] = o['contract_name']
        return_data[i]['last_update'] = o['last_update']
        #get optional arguments
        for a in args:
            #print a
            return_data[i][a] = o[a] #add a test here
        i=i+1
    return return_data

def parseStaticData(str_data,*args):
    obj_data = json.loads(str_data)
    return_data = {}
    for a in args:
        print a
        return_data[a] = obj_data[a] #add a test here
    return return_data

def listAllParams(str_data,print_data = False):
    #function to list all available parameters, optionally printing these to screen
    #need some way of handling recursion if this is to be really useable
    obj_data = json.loads(str_data)
    if print_data == True:
        for o in obj_data:
            print o

def poll_data():
    #main function to poll data for contract and write to file
    get_data = getAllStationsData(key,contract)
    logging.debug( str(time())+"@POLL_DATA - post getAllStationsData")
    clean_data = parseAllStationData(get_data,'available_bikes','available_bike_stands')
    logging.debug( str(time())+"@POLL_DATA - after parsing Data")
    #now write data - could separate this out. Write to SQL, AWS etc..
    for d in clean_data:
        #print d
        clean_data[d]['local_timestamp'] = time()
        with open(outputfile, 'a') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writerow(clean_data[d])

###
# Check output file
###

print "Running "+__file__

#does the output file exists. If not then create csv
fields = ['number','contract_name','last_update','available_bikes','available_bike_stands','local_timestamp']

try:
    #try to open the file
    f = open(outputfile, 'r')
    f.close
except:
    #if it will not open then create it using template list above
    print("Creating CSV file")
    f = open(outputfile, 'w')
    outputstr = ','.join(fields)
    f.write(outputstr)
    f.close()
    #if it doesnt open then create it

###
#Run data through
###

logging.info( "Starting "+str(time()))

attempts=0
while True and attempts<=max_attempts:
    try:
        poll_data()
        sleep(delay)
        attempts=0
    except:
        logging.error( str(time())+"Error in poll_data - error count "+str(attempts))
        sleep(errdelay)
        errdelay=errdelay* 2
        logging.debug(str(time())+"Delay is "+str(errdelay))
        attempts = attempts+1
logging.error( "Quitting "+str(time()))


###TO DO
# Make file handling a little bit less hard coded. eg  list of fields in config separated into all bonus
# Pass arguments from cmd
# Make it object oriented
# Write out to multiple file formats SQL,AWS etc..                       
# Some sort of status check - maybe an ec2 instance?
# Assocaite city and api file with input params

###DONE
# Setup error logging
#  Setup timer to query at intervals ; query error intervals as well
# Bring main running into a function
# Import API so it is not on GIT


##CANCEL
# Maybe join the parse and query functions? No, worth distinguishing these