#importing all the necessary libraries and methods

from access import CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET , ACCOUNT_NAME
from textToInfo import *
from sheetApi import *
from pprint import pprint
import sys
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import json


ERROR = "Please check your tweet , and retweet"

# this class is used to listen to
class StdOutListener(StreamListener):
    #this function is used to get data from twitter and then call respondToTweet function to reply to the tweet
    def on_data(self , data):
        #data is stored as json file 
        clean_data = json.loads(data)

        #id of the user is stored here
        tweetId = clean_data['id']

        # tweet text is extracted 
        text = clean_data['text']

        #location is extracted from the text using text_preproc function  
        text = text_preproc(text).title()

        #we use the findLocation function to know if we have this location or not
        state , where = findLocation(text)

        #if invalid location then error message is tweeted
        if(state == ERROR):
            respondToTweet(ERROR , tweetId)
        else:
            #calling the getCovidInfo to get all the necessary info of the location 
            tweet = getCovidInfo(state,where)

            # calling the respond to tweet function to reply to the user with necessary tweet
            respondToTweet(tweet , tweetId)
        



#this method is used to create the tweepy api using the credentials obtained from twitter developer account
def createTweepyApi():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    
    #creating the api
    api = tweepy.API(auth)
    try:
        #if api credentials are verified
        api.verify_credentials()
        print('Authentication Successful')
    except:
        #if any error in verifying the credentials
        print('Error while authenticating API')
        sys.exit(1)    
    # returning the api object and authentication credentials auth
    return api , auth

#This function is used call the function to create tweepy api and listen to tweets only tweeted at ACCOUNT_NAME(i.e askCovidIndia)
def followStream():

    api , auth = createTweepyApi()
    #creatinf an instance of StdOutListener class
    listener = StdOutListener()

    #creating an instance of stream and passing our authentication credentials 
    #and listener so that our callback functions are called
    stream = Stream(auth , listener)

    # listening to tweets which has only ACCOUNT_NAME tagged in it
    stream.filter(track = [ACCOUNT_NAME])


#This function takes in the actual reply text tweet and tweets it back to the person with account tweetId
def respondToTweet(tweet , tweeId):
    api, auth = createTweepyApi()

    #tweeting the tweet to id = tweetId
    api.update_status(tweet , in_reply_to_status_id = tweeId , auto_populate_reply_metadata = True)


if __name__ == '__main__':
    # Create API object
    followStream()