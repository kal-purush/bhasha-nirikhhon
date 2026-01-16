#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# TweetMachine project - https://github.com/lucach/tweetmachine
# Copyright © 2014 Demetrio Carrara <carrarademetrio@gmail.com>
# Copyright © 2014 Luca Chiodini <luca@chiodini.org>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import codecs
import time
import traceback
import tweepy

# ==== CONFIG SECTION ====
CONSUMER_KEY = YOUR_CONSUMER_KEY
CONSUMER_SECRET = YOUR_CONSUMER_SECRET
ACCESS_TOKEN = YOUR_ACCESS_TOKEN
ACCESS_TOKEN_SECRET = YOUR_ACCESS_TOKEN_SECRET
HASHTAG = YOUR_HASHTAG
TWEETS_NUMBER = 5
OUTPUT_FILENAME = "output.txt"

# Try to perform an OAuth with Twitter.
# TODO Catch exceptions.
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

while 1:
    try:

        # Search tweets with the provided hashtag.
        # Exclude retweets.
        # Limit to the TWEETS_NUMBER most recent tweets.

        tweets = api.search("#" + HASHTAG + " +exclude:retweets",
                            result_type="recent", count=TWEETS_NUMBER)

        if tweets:
            out_file = codecs.open("output.txt", "w", "utf-8")
            out_file.write(" *** ")

            # For each tweet.
            for tweet in tweets:
                # Write the tweet on file.
                out_file.write(tweet.text)
                # Add a custom separator.
                out_file.write(" *** ")

            out_file.close()

    except:
        # For some reason (no futher investigation) we have failed tweets
        # retriving. Let's be verbose but proceed anyway with next loop.
        # Also, we print the traceback stack as it may be very useful for
        # debug.
        print "Tweet update failed. Double check your internet connection."
        print traceback.format_exc()

        pass

    # Wait a minute before doing another loop
    time.sleep(60)