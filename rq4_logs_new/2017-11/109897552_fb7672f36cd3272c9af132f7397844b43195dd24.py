import requests, json
from urllib.request import *


def getList(url):
    ID = url[url.find('id=') + len('id='):]
    url = 'http://music.163.com/api/playlist/detail?id=' + ID
    r = requests.get(url)
    data = json.loads(r.text)
    l = []
    for track in data['result']['tracks']:
        songName = track['name']

        artistsName = ''
        for artist in track['artists']:
            artistsName += artist['name'] + '/'
        artistsName = artistsName[:-1]
        # print artistsname

        albumName = track['album']['name']
        l.append((songName, artistsName, albumName))
    return l

# print(getList('http://music.163.com/#/playlist?id=2673111'))