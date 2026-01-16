from urllib.request import Request, urlopen
import json

REDDIT_URL = "https://www.reddit.com"
SUB_TEMPLATE = REDDIT_URL + "/r/{subreddit}.json"

class subreddit(object):
    def __init__(self, sub_name):
        self.name = sub_name
        self.raw  = None
        self.json = {}

        self.posts = None
    
    def __str__(self):
        return '/r/%s' % self.name
    
    def loadData(self):
        req = Request(SUB_TEMPLATE.replace('{subreddit}',
                                           self.name))
        req.add_header('User-Agent', 'super happy flair bot by /u/spladug')
        raw = urlopen(req)
        rawData = raw.read().decode("utf-8")
        jsonData = json.loads(rawData)
        
        self.raw = rawData
        self.json = jsonData

        jsonData = jsonData.get('data')
        if jsonData:
            self.posts = jsonData['children']
        
    def get(self, elem):
        return self.__dict__.get(elem)