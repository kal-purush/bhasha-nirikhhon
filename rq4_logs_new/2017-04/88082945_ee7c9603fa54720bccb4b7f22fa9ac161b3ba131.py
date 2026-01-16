import sys,requests
from wit import Wit
from goibibo import goibiboAPI
from iata_codes import IATACodesClient

'''
if len(sys.argv) != 2:
    print('usage: python ' + sys.argv[0] + ' <wit-token>')
    exit(1)
'''
access_token = 'XL7BFW47QBDKM5T6KE47KXA2TNQYLPBN'
IATA_KEY = 'fe8ab715-e164-4262-a508-3e9b5db940df'
APP_ID = 'a3649501'
APP_KEY = '4ef08768f5a9234ce7262fdea9401808'
BASE = "http://developer.goibibo.com/api/"
auth = {'app_id':APP_ID,'app_key':APP_KEY}

iataClient = IATACodesClient(IATA_KEY)
goibiboClient = goibiboAPI(APP_ID,APP_KEY)

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

def ifdatetime(entities,entity='datetime'):
    if entity not in entities:
        return None
    val = entities[entity][0]['value']
    if not val:
        return None
    return val['value'] if isinstance(val, dict) else val    

def send(request, response):
    print(response['text'])

def searchTransport(request):
    
    context = request['context']
    entities = request['entities']

    source = ifsource(entities)
    destination = ifdestination(entities)
    datetime = ifdatetime(entities)
    

    if source and destination and datetime:

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

        
        context['transportList'] = url
        try:
            del context['missingDateTime']
            del context['missingLocation']
        except:
            pass

    elif datetime is None:
        context['missingDateTime'] = True
        try:
            del context['missingLocation']
            del context['transportList']
        except:
            pass

    else:
        context['missingLocation'] = True
        try:
            del context['missingDateTime']
            del context['transportList']
        except:
            pass
   # print(context)
    return context

actions = {
    'send': send,
    'searchTransport': searchTransport,
}

client = Wit(access_token=access_token, actions=actions)
client.interactive()