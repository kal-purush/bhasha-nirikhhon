import requests
import csv
from iata_codes import IATACodesClient

IATA_KEY = 'fe8ab715-e164-4262-a508-3e9b5db940df'
APP_ID = 'a3649501'
APP_KEY = '4ef08768f5a9234ce7262fdea9401808'
BASE = "http://developer.goibibo.com/api/"
auth = {'app_id':APP_ID,'app_key':APP_KEY}

iataClient = IATACodesClient(IATA_KEY)


def ifsource(entities,entity='location'):
    if entity not in entities:
        return None
    val = entities[entity][0]['value']
    if not val:
        return None
    return val['value'] if isinstance(val, dict) else val

def ifdestination(entities,entity='location'):
    if entity not in entities:
        return None
    val = entities[entity][1]['value']
    if not val:
        return None
    return val['value'] if isinstance(val, dict) else val

def iftransport(entities, entity='transport'):
    if entity not in entities:
        return None
    val = entities[entity][0]['value']
    if not val:
        return None
    return val['value'] if isinstance(val, dict) else val

def ifdatetime(entities,entity='datetime'):
    if entity not in entities:
        return None
    val = entities[entity][0]['value']
    if not val:
        return None
    return val['value'] if isinstance(val, dict) else val

def ifcheckin_checkout(entities,entity='datetime'):
    if entity not in entities:
        return None
    try:
        val = (entities[entity][0]['from']['value'],entities[entity][0]['to']['value'])
    except:
        return (None,None)
    if not val:
        return (None,None)
    return (val[0]['value'],val[1]['value']) if isinstance(val[0],dict) else val

def searchFlight(source, destination, datetime):
    codesource = iataClient.get(name=source)[0]['code']
    codedest = iataClient.get(name=destination)[0]['code']
    DOD = datetime[0:4] + datetime[5:7] + datetime[8:10]
    DOD = int(DOD)
    dateda = "&dateofdeparture=%d" % DOD
    URL = BASE + "search/" + "?format=json" + "&source=%s" % codesource + "&destination=%s" % codedest + dateda + '&app_id=' + APP_ID + '&app_key=' + APP_KEY + '&seatingclass=E&adults=1&children=0&infants=0&counter=100'
    data = requests.get(URL).json()
    print(str(URL))
    data = data['data']['onwardflights'][0]['CINFO']
    print('DOD is :'+str(DOD) + 'And the codes are :' + codesource + " "+ codedest)
    url = 'https://www.goibibo.com/flights/' + data
    
    return url

def searchBus(source, destination, datetime):
    DOD = datetime[0:4] + datetime[5:7] + datetime[8:10]
    URL = BASE + 'bus/search/?format=json' + '&app_id=' + APP_ID + '&app_key=' + APP_KEY + '&source='+  source + '&destination=' + destination + '&dateofdeparture=' + DOD
    data = requests.get(URL).json()
    data = data['data']['onwardflights']
    src_voyager_id = data[0]['src_voyager_id']
    dest_voyager_id = data[0]['dest_voyager_id']
    url = 'https://www.goibibo.com/bus/#bus-' + source + '-' + destination + '-' + DOD + '---0-0-' + src_voyager_id + '-' + dest_voyager_id
    return url

def search_hotels(location, check_in_date, check_out_date):
    ci = check_in_date[0:4] + check_in_date[5:7] + check_in_date[8:10]
    co = check_out_date[0:4] + check_out_date[5:7] + check_out_date[8:10]
    city_id = None
    with open('city_list.csv', 'r') as csvfile:
        data = csv.reader(csvfile)
        for row in data:
            if location.lower() == row[0].lower():
                city_id = row[1]

    base_url = 'https://www.goibibo.com/hotels/find-hotels-in-' + location.lower() + '/' + city_id + '/' + city_id
    url = base_url + '/%7B"ci":"' + ci + '","co":"'+ co + '","r":"1-1_0"%7D/?{}&sec=dom'
    return url
    