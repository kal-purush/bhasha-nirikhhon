import requests
import json
import pandas as pd
import time

def get_page(address):
    ak = 'gfC2ZNiGIwglL3nO4mU7GsRazDuEDge5'
    url = 'http://api.map.baidu.com/place/v2/search?' \
          'query={address}&region=合肥&output=json&ak={ak}'.format(address=address, ak=ak)
    response = requests.get(url)
    try:
        return json.loads(response.text)
    except Exception as e:
        print(e)
        return None


def get_location(page):
    try:
        lng = page['results'][0]['location']['lng']
        lat = page['results'][0]['location']['lat']
        return lng, lat
    except BaseException as e:
        return 0, 0


if __name__ == '__main__':
    house = pd.read_csv('communitites.csv', encoding='utf-8')
    address = house['community']
    print(len(address))
    dic = {}
    locations = []
    i = 1
    for addr in address:
        if dic.get(addr) is None:
            page = get_page(addr)
            if page is not None:
                location = get_location(page)
                loc = str(location).strip('()')
            else:
                loc = '0,0'
            locations.append(loc)
            dic[addr] = loc
            print(i, addr, loc)
            i += 1
            time.sleep(0.5)
        else:
            locations.append(dic[addr])
    print(len(locations))
    loc_column = pd.Series(locations, name='location')
    save = pd.DataFrame({'location': loc_column})
    save.to_csv("locations.csv", encoding='utf-8', columns=['location'], header=True, index=False)