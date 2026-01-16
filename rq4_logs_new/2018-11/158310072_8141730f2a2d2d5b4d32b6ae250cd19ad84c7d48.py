import urllib.request
import json


def searchResults(stockData):

    url = "https://www.alphavantage.co/query?"
    function = "function=TIME_SERIES_DAILY"
    interval = "interval=1min"
    apikey = "apikey=MINCN67P0HGU1H6Q"
    test = []
    a = " "
    symbol1 = "symbol=" + a
    fullURL = url + function + "&" + symbol1 + "&" + interval + "&" + apikey
    counter = 0
    track = []
    info = []
    for x in range(len(stockData)):
        if counter == 4:
            break
        a = stockData[x]["1. symbol"]
        symbol1 = "symbol=" + a
        fullURL = url + function + "&" + symbol1 + "&" + interval + "&" + apikey
        resp = urllib.request.urlopen(fullURL)
        result = json.loads(resp.read())
        if 'Time Series (Daily)' in result:
            track.append(x)
            counter += 1
            test.insert(x, 1)
            info.append(result)
        else:
            counter += 1
            test.insert(x, 0)
    return test, track, info