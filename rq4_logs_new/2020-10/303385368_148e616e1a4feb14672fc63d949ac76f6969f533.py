import tweepy
import settings
from textblob import TextBlob
import dataset
import json
import utility
from datetime import datetime


class StreamListener(tweepy.StreamListener):
    def __init__(self, date="tweets"):
        self.tot_counter = 0
        self.spec_counter = [0] * len(settings.TRACK_TERMS)
        self.ending = "\033[A" * (len(settings.TRACK_TERMS) + 2) + "\r"
        self.date = date
        super(StreamListener, self).__init__()

    def update_counters(self, text, hashtags, urls, quoted_text, quoted_hashtags, quoted_urls):
        string_counter = "--TAGs COUNTER--\n"
        for e in settings.TRACK_TERMS:
            if e in text.lower() or e in json.dumps(hashtags).lower() or e in json.dumps(urls).lower()\
                    or e in quoted_text.lower() or e in json.dumps(quoted_hashtags).lower() \
                    or e in json.dumps(quoted_urls).lower():
                self.spec_counter[settings.TRACK_TERMS.index(e)] += 1
            string_counter += "  {}:{}\n".format(e, self.spec_counter[settings.TRACK_TERMS.index(e)])
        return string_counter

    def on_status(self, status):
        # Skipping retweets
        if hasattr(status, 'retweeted_status'):
            return

        # the description the user who created the tweet wrote in their biography
        description = status.user.description
        # the location the user who created the tweet wrote in their biography.
        loc = status.user.location
        # IF tweets longer than 140, the text is truncated
        truncated = status.truncated
        # The text, hashtags and URL of the tweet (dump JSON dict to string)
        if truncated:
            text = status.extended_tweet['full_text']
            hashtags = ""
            for h in status.extended_tweet['entities']['hashtags']:
                hashtags += " {}".format(h['text'])
            urls = ""
            for u in status.extended_tweet['entities']['urls']:
                urls += " {}".format(u['expanded_url'])
        else:
            text = status.text
            hashtags = ""
            for h in status.entities['hashtags']:
                hashtags += " {}".format(h['text'])
            urls = ""
            for u in status.entities['urls']:
                urls += " {}".format(u['expanded_url'])
        # The geographic coordinates from where the tweet was sent.
        coords = status.coordinates
        # dump coordinates json dictionary to a string, so we can store it:
        if coords is not None:
            coords = json.dumps(coords)
        # The screen name of the user
        name = status.user.screen_name
        # When the user’s account was created
        user_created = status.user.created_at
        # How many followers the user has
        followers = status.user.followers_count
        # The unique id that Twitter assigned to the tweet
        id_str = status.id_str
        # When the tweet was sent
        created = status.created_at
        # IF the tweet is quoting some other, store some info of the quoted one
        if hasattr(status, 'quoted_status'):
            quoted = True
            if status.quoted_status.truncated:
                quoted_text = status.quoted_status.extended_tweet['full_text']
                quoted_hashtags = ""
                for h in status.quoted_status.extended_tweet['entities']['hashtags']:
                    quoted_hashtags += " {}".format(h['text'])
                quoted_urls = ""
                for u in status.quoted_status.extended_tweet['entities']['urls']:
                    quoted_urls += " {}".format(u['expanded_url'])
            else:
                quoted_text = status.quoted_status.text
                quoted_hashtags = ""
                for h in status.quoted_status.entities['hashtags']:
                    quoted_hashtags += " {}".format(h['text'])
                quoted_urls = ""
                for u in status.quoted_status.entities['urls']:
                    quoted_urls += " {}".format(u['expanded_url'])
        else:
            quoted = False
            quoted_text = ""
            quoted_hashtags = ""
            quoted_urls = ""

        # Sentiment Analysis
        # TODO Doing in post-processing otherwise slowing down the streaming buffer consume
        blob = TextBlob(text)
        sent = blob.sentiment
        # polarity is the negativity or positivity of the tweet, on a -1 to 1 scale.
        polarity = sent.polarity
        # subjectivity is how objective or subjective the tweet is.
        # 0 means that the tweet is very objective, and 1 means that it is very subjective.
        subjectivity = sent.subjectivity

        # Using SQLite, if the database file doesn’t exist, it will be automatically created in the current folder
        db = dataset.connect(settings.CONNECTION_STRING.format(self.date))
        table = db[settings.TABLE_NAME]
        table.insert(dict(
            user_description=description,
            user_location=loc,
            coordinates=coords,
            truncated=truncated,
            text=text,
            hashtags=hashtags,
            urls=urls,
            user_name=name,
            user_created=user_created,
            user_followers=followers,
            id_str=id_str,
            created=created,
            polarity=polarity,
            subjectivity=subjectivity,
            quoted=quoted,
            quoted_text=quoted_text,
            quoted_hashtags=quoted_hashtags,
            quoted_urls=quoted_urls
        ))

        # PRINT some dynamic result (mainly for debugging purpose)
        # TODO: Open new thread to count this OR removing this part
        self.tot_counter += 1
        print("DATE {} - tweet received: {}\n{}".format(self.date, self.tot_counter,
                                                        self.update_counters(text, hashtags, urls, quoted_text,
                                                                             quoted_hashtags, quoted_urls)),
              end=self.ending)

        if self.tot_counter % 5000 == 0:
            now = datetime.now()
            today = "{:02d}{:02d}/{:02d}{:02d}{:02d}".format(now.month, now.year, now.day, now.month, now.year)
            utility.print_on_file("TIME {:02d}:{:02d} COLLECTED {} tweets".format(now.hour, now.minute,
                                                                                  self.tot_counter), today)
            if self.date != str(today):
                return False

    def on_error(self, status_code):
        # 420 for rate limit
        if status_code == 420:
            return False