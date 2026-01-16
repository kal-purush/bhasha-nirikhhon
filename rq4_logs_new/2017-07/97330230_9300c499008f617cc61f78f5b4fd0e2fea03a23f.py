import os
from markovbot import MarkovBot
import json

tweetbot = MarkovBot()

dirname = os.path.dirname(os.path.abspath(__file__))
book = os.path.join(dirname, 'The Dhammapada.txt')
tweetbot.read(book)

my_first_text = tweetbot.generate_text(25, seedword=['way', 'He'])
print("tweetbot says:")
print(my_first_text)

json_data = {}

with open("config.json") as json_file:
    json_data = json.loads(json_file.read())
    print(json_data)

consumer_key = json_data['consumer_key']
consumer_secret = json_data['consumer_secret']
access_token = json_data['access_token']
access_token_secret= json_data['access_token_secret']
tweetbot.twitter_login(consumer_key, consumer_secret, access_token, access_token_secret)

targetstring = 'Buddhism'
keywords= ['man', 'always', 'truth', 'ignorant', 'lives']
prefix = None
suffix= '#BuddhismSays'
maxconvdepth = None

tweetbot.twitter_autoreply_start(targetstring, keywords=keywords, prefix=prefix, suffix=suffix, maxconvdepth=maxconvdepth)


tweetbot.twitter_tweeting_start(days=1, hours=6, minutes=5, keywords=None, prefix=None, suffix=None)