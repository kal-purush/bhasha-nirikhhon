from urllib.request import Request, urlopen
import json

REDDIT_URL = "https://www.reddit.com"

class post(object):
    def __init__(self, post_url, post_name='default'):
        self.name = post_name
        if REDDIT_URL in post_url:
            self.url  = post_url
        else:
            self.url = REDDIT_URL + post_url

        self.raw = None
        self.json = None

        self.comments = None

    def __str__(self):
        return self.name

    def loadData(self):
        req = Request(self.url + '.json')

        req.add_header('User-Agent', 'super happy flair bot by /u/spladug')
        raw = urlopen(req)

        rawData = raw.read().decode("utf-8")
        jsonData = json.loads(rawData)

        self.raw = rawData
        self.json = jsonData
        if len(jsonData) > 1:
            self.comments = jsonData[1]['data']['children']