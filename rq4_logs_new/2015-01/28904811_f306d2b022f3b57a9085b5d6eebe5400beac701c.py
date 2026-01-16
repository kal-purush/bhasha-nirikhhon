#************************************************************************
# Name: Eisha Lee
# date: November 20th, 2013
# file: TweetSentiment.py
#
# Represents tweets and zip info as dictionaries and finds state and
# zip code information for each tweet
#************************************************************************
import datetime as dt
import string
import csv
import math

# Phase 1

def make_tweet(tweet_line):
    """Return a tweet, represented as a python dictionary.
    tweet_line: a string corresponding to a line formatted as in all_tweets.txt

    Dictionary keys:
    text  -- A string; the text of the tweet, all in lowercase
    time  -- A datetime object; the time that the tweet was posted
    lat   -- A number; the latitude of the tweet's location
    lon   -- A number; the longitude of the tweet's location

    """
    tweet = tweet_line.strip().split("\t")
    if len(tweet) >= 4:
        lat, lon = eval(tweet[0])
        time = dt.datetime.strptime(tweet[2], '%Y-%m-%d %H:%M:%S')
        text = tweet[3].lower()
        return {'text':text, 'time':time, 'latitude':lat, 'longitude':lon}
    
def tweet_text(tweet):
    """Return the text of a tweet as a string"""
    return tweet['text']

def tweet_words(tweet):
    """Return a list of the words in the text of a tweet not
    including punctuation."""
    text = tweet['text'].translate(string.maketrans("",""), string.punctuation)
    return text.split(' ')

def tweet_time(tweet):
    """Return the datetime that represents when the tweet was posted."""
    return tweet['time']

def tweet_location(tweet):
    """Return an tuple that represents the tweet's location."""
    return (tweet['latitude'], tweet['longitude'])
    
    
def make_zip(zipcode):
    """Return a zip code, represented as a python dictionary.
    zipcode: a list containing a single zip codes data ordered as in zips.csv

    Dictionary keys:
    zip    -- A string; the sip code
    state   -- A string; Two-letter postal code for state
    lat    -- A number; latitude of zip code location
    lon    -- A number; longitude of zip code location
    city   -- A string; name of city assoicated with zip code
    """    
    zip_string = zipcode[0]
    state = zipcode[1]
    lat = float(zipcode[2])
    lon = float(zipcode[3])
    city = zipcode[4]
    return {'zip':zip_string, 'state':state, 'latitude':lat, \
        'longitude':lon, 'city':city}

def find_zip(tweet, zip_list):
    """return zipcode associated with a tweets location data
    zip_list is a list of zip_codes represented as dictionaries"""
    tweet_loc = tweet_location(tweet)
    zip_info = {}
    min_dist = 10000000    # arbitrary high number
    for item in zip_list:
        zip_loc = (item['latitude'], item['longitude'])
        distance = geo_distance(tweet_loc, zip_loc)
        if (distance < min_dist):
            min_dist = distance
            zip_info = item
    return zip_info    

def geo_distance(loc1,loc2):
    """Return the great circle distance (in miles) between two
    tuples of (latitude,longitude)

    Uses the "haversine" formula.
    http://en.wikipedia.org/wiki/Haversine_formula"""
    radius = 3963.1676    # earth radius in miles
    lat1 = math.radians(float(loc1[0]))
    long1 = math.radians(float(loc1[1]))
    lat2 = math.radians(float(loc2[0]))
    long2 = math.radians(float(loc1[1]))
    dlat = lat2 - lat1
    dlon = long2 - long1
    a = math.sin(dlat/2) ** 2  + math.sin(dlon/2) ** 2 * \
        math.cos(lat1) * math.cos(lat2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));
    distance = c * radius
    return distance

def add_geo(tweets):
    """adds the new keys state and zip to each tweet dictionary 
    in the list tweets"""
    zipcsv = raw_input('Enter name of zip file: ')
    zip_dict = []
    for element in zip_reader(zipcsv):
        zip_dict.append(make_zip(element))
    for item in tweets:
        zip_info = {}
        zip_info.update(find_zip(item, zip_dict))
        item.update({'state':zip_info['state'], 'zip':zip_info['zip']})

def write_tweets(tweets, outfile):
    """writes the list of tweets to a text file with name outfile"""
    out = open(outfile, 'w')
    for item in tweets:
        text = '[' + str(item['latitude']) + ', ' + str(item['longitude'])\
            + ']\t' + str(item['time']) + '\t' + str(item['state']) + '\t'\
            + str(item['zip']) + '\t' + str(item['sentiment']) + '\t'\
            + str(item['text']) + '\n'
        out.write(text)
    out.close()

def zip_reader(zip_file):
    """reads csv zip file and returns list of zip code info"""
    csvfile = open(zip_file,'r')
    zips = []
    csvfile.readline()
    for row in csv.reader(csvfile, delimiter=','):
        zips.append([item.replace('"', '').strip() for item in row])
    csvfile.close()
    return zips

def tweet_reader(tweet_file):
    """reads text file and returns string representation line of text"""
    infile = open(tweet_file, 'r')
    tweets = []
    for line in infile:        
        tweets.append(line)
    infile.close()
    return tweets
    
# Phase 2  

def sentiment_reader(sent_file):
    """reads sentiment value file and returns it as a dictionary"""
    csvfile = open(sent_file, 'r')
    sentiment_dict = {}
    for row in csv.reader(csvfile, delimiter=','):
        sentiment_dict[row[0]] = float(row[1])
    return sentiment_dict
    
def sentiment_value(word, sentiments):
    """checks if sentiment dictionary has word and returns its sentiment 
    value, returns None if it does not"""
    if (sentiments.has_key(word)):
        return float(sentiments[word])
    else:
        return None

def add_sentiment(sentiments, tweets):
    """ adds sentiment values to list of tweets"""
    for item in tweets:
        total = 0.0
        count = 0.0
        ave = None
        words = tweet_words(item)
        for word in words:
            sent_val = sentiment_value(word, sentiments)
            if sent_val != None:
                total += sent_val
                count += 1.0
                ave = total/count
        item.update({'sentiment':ave})

def tweet_filter(tweets, **kwargs):
    """fliters list of tweets by given key word arguments"""
    filtered_tweets = []    
    if len(kwargs) == 1:        
        if kwargs.has_key('word'):
            for item in tweets:
                if kwargs['word'] in tweet_words(item):
                    filtered_tweets.append(item)
        elif kwargs.has_key('state'):
            for item in tweets:
                if kwargs['state'] == item['state']:
                    filtered_tweets.append(item)
        else:
            for item in tweets:
                if kwargs['zip'] == item['zip']:
                    filtered_tweets.append(item)        
    elif len(kwargs) == 2:
        if kwargs.has_key('word') and kwargs.has_key('state'):
            for item in tweets:
                if kwargs['word'] in tweet_words(item) and item['state'] \
                == kwargs['state']:
                    filtered_tweets.append(item)                
        elif kwargs.has_key('word') and kwargs.has_key('zip'):
            for item in tweets:
                if kwargs['word'] in tweet_words(item) and item['zip'] \
                == kwargs['zip']:
                    filtered_tweets.append(item)
        else:
            for item in tweets:
                if item['state'] == kwargs['state'] and item['zip'] \
                == kwargs['zip']:
                    filtered_tweets.append(item)            
    else:
        for item in tweets:
            if kwargs['word'] in tweet_words(item) and item['state'] \
            == kwargs['state'] and item['zip'] == kwargs['state']:
                  filtered_tweets.append(item)   
    return filtered_tweets
    
def average_sentiment(filtered_tweets):
    """given list of filtered tweets, returns average sentiment value"""
    if len(filtered_tweets) == 0:
        return 'No average: List is empty'
    else:
        avglist = []
        avg = None
        for item in filtered_tweets:
            sentiment = item['sentiment']
            if sentiment != None:
                avglist.append(item)
        if len(avglist) != 0:
            avg = sum(avglist)/len(avglist)
        return avg
    
def filter_by_state(filtered_tweets):
    """returns a dictionary filtering by state
    key: state postal code value: list of tweets in state"""
    state_filter = {}
    for item in filtered_tweets:
        state = item['state']
        if state_filter.has_key(state):
            state_filter[state].append(item)
        else:
            tweet_list = []
            tweet_list.append(item)
            state_filter.update({str(state): tweet_list})
    return state_filter

def most_positive(tweets, word): 
    """returns the state with the highest average sentiment given word"""
    filtered_list = tweet_filter(tweets, word = word)
    filtered_state = filter_by_state(filtered_list)
    max_sent = 0.0
    state = ''
    for key in filtered_state.keys():
        value_list = filtered_state[key]
        sentiment_value = average_sentiment(value_list)
        if sentiment_value > max_sent:
            max_sent = sentiment_value
            state = str(key)
    return state

def most_negative(tweets, word): 
    """returns the state with the lowest average sentiment given word"""
    filtered_list = tweet_filter(tweets, word = word)
    filtered_state = filter_by_state(filtered_list)
    min_sent = 1000.0
    state = ''
    for key in filtered_state.keys():
        value_list = filtered_state[key]
        sentiment_value = average_sentiment(value_list)
        if sentiment_value < min_sent:
            min_sent = sentiment_value
            state = str(key)
    return state