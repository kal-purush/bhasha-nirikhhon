#!/usr/bin/env python

import os
import json
import codecs
import sys

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from haiku import Haiku

access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']


class HaikuListener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        sys.stdout.flush()

        if not (tweet.get('lang') and tweet['lang'] == 'en'):
            print 'F',
            return

        try:
            haiku = Haiku(tweet['text'])
            if haiku.is_valid():
                print
                print haiku.formatted()
                sys.exit()

        except:
            print 'E',
            return

        print '.',


if __name__ == '__main__':

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, HaikuListener())

    stream.sample(languages=["en"])