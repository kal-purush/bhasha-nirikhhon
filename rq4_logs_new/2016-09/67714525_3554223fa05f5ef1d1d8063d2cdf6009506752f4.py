"""
Okay, the idea for this is to just keep track in ten minute increments 
the total volume of the tweets for these monsters
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tweepy 
import json
import sys
from datetime import date
import codecs
import time
import csv


class MyStreamListener(tweepy.StreamListener):

        def __init__(self, file_name):
                self.write_file = file_name
		self.count = 0
		self.hillary_count = 0
		self.trump_count = 0
		self.both_count = 0
		self.neither_count = 0
		self.start_time = time.time()
	
	def test_tweet(self, tweet_test):
		tweet_text = tweet_test['text'].lower()
		if u'hillary' in tweet_text:
			if u'trump' in tweet_text:
				return 'b'
			else:
				return 'h'
		elif u'trump' in tweet_text:
			return 't'
		else:
			return 'n'

	def on_data(self, data):
		# if it's been ten minutes since an update
		if time.time() - self.start_time > 600.0:

			# reset timer
			self.start_time = time.time()
			with codecs.open(self.write_file, 'a', 'utf-8') as write_out:
				csv_writer = csv.writer(write_out, delimiter=',')
				csv_writer.writerow([str(time.localtime()[i]) for i in range(5)] + [str(self.count)]
					+ [str(self.hillary_count), str(self.trump_count), str(self.both_count), str(self.neither_count)])
			# reset counters
			self.count = 0
			self.hillary_count = 0
			self.trump_count = 0
			self.neither_count = 0
			self.both_count = 0
			
		
		tweet = json.loads(data)
                try:
			if "RT" not in tweet["text"] and "https://t.co" not in tweet["text"]:
				self.count += 1
				whois = self.test_tweet(tweet)
				if whois == 'h':
					self.hillary_count += 1
				elif whois == 't':
					self.trump_count += 1
				elif whois == 'b':
					self.both_count += 1
				else:
					self.neither_count += 1
                except KeyError:
			pass

	def on_error(self, status):
		print (status)

def main():
	CONSUMER_KEY = 'SE5gaC6dWv4gWLOv9kNavyvz2'
	CONSUMER_SECRET = 'iCuXVKtvKa3L3mKfWznAv1YV7IRLa8CmAmWZ5Iudvx9yavJE1p'
	ACCESS_KEY = '755408587200102400-X4i55C0tGRR1KBTC9ZNBfC7hjk4X24o'
	ACCESS_SECRET = 'IQqrlRfw9JZhi0WkKL0OcQECOxbdZ9eNBNYJQzpY0PQUs'

	auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
	api = tweepy.API(auth)

        #these are what we're filtering for
        to_filter = ['Hillary', 'Trump']
	filename = 'total_volume.csv'

	myStreamListener = MyStreamListener(filename)
	myStream = tweepy.Stream(auth, myStreamListener)


	myStream.filter(track=to_filter)

if __name__ == "__main__":
	main()