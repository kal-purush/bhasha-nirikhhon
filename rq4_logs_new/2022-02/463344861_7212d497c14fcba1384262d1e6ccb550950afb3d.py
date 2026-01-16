#imports
import praw
import pandas as pd
import warnings
import enum

from credentials import *

#setup
warnings.filterwarnings("ignore")

#ENUMS
class redditTimeFilter(enum.Enum):
    ALL = "all"
    DAY = "day"
    HOUR = "hour"
    MONTH = "month"
    WEEK = "week"
    YEAR = "year"

class redditAPI:
  
    redditApi = None
    
    def __init__(self):
        
        self.redditApi = praw.Reddit(
            client_id = REDDIT_CLIENT_ID,
            client_secret = REDDIT_SECRET_TOKEN,
            password = REDDIT_PASSWORD,
            username = REDDIT_USERNAME,
            user_agent="USERAGENT"  
        )
    
    #Subreddit Titles
    def getTopSubredditTitles(self, subredditName, timeFilter):

        df = pd.DataFrame()

        subreddit = self.redditApi.subreddit(subredditName)
        for post in subreddit.top(timeFilter):
            df = df.append({
                'subredditName' : subreddit.display_name, 
                'subredditId' : subreddit.id , 
                'subredditType' : "Top", 
                'subredditTimeFilter' : timeFilter, 
                'postTitle' : post.title, 
                'postId' : post.id, 
                'postScore' : post.score, 
                'postNumOfComments' : post.num_comments, 
                'postCreated' : pd.to_datetime(post.created, unit="s") 
            }, ignore_index = True)

        return df

    def getHotSubredditTitles(self, subredditName, timeFilter):

        df = pd.DataFrame()

        subreddit = self.redditApi.subreddit(subredditName)
        for post in subreddit.hot():
            df = df.append({
                'subredditName' : subreddit.display_name, 
                'subredditId' : subreddit.id , 
                'subredditType' : "Hot",  
                'postTitle' : post.title, 
                'postId' : post.id, 
                'postScore' : post.score, 
                'postNumOfComments' : post.num_comments, 
                'postCreated' : pd.to_datetime(post.created, unit="s") 
            }, ignore_index = True)

        return df

    def getNewSubredditTitles(self, subredditName, timeFilter):

        df = pd.DataFrame()

        subreddit = self.redditApi.subreddit(subredditName)
        for post in subreddit.new():
            df = df.append({
                'subredditName' : subreddit.display_name, 
                'subredditId' : subreddit.id , 
                'subredditType' : "New",  
                'postTitle' : post.title, 
                'postId' : post.id, 
                'postScore' : post.score, 
                'postNumOfComments' : post.num_comments, 
                'postCreated' : pd.to_datetime(post.created, unit="s") 
            }, ignore_index = True)

        return df



p1 = redditAPI()
df = p1.getTopSubredditTitles("stocks", redditTimeFilter.WEEK.value)
df.to_csv("stocks.csv")

