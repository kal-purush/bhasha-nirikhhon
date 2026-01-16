#imports
import requests
from credentials import *



def getRedditHeader():
    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET_TOKEN)
    data = {'grant_type': 'password', 'username': REDDIT_USERNAME, 'password': REDDIT_PASSWORD}
    headers = {'User-Agent': 'StockAnalyser/0.0.1'}
    res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
    TOKEN = res.json()['access_token']
    headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

    return headers

#headers = getRedditHeader()
#res = requests.get("https://oauth.reddit.com/r/python/hot", headers=headers)
#print(res.json())
