#imports
from os.path import exists
import os
import pandas as pd

from redditAPI import redditTimeFilter
from redditAPI import redditAPI


def main():
    
    print("Starting Stock Analyser")
    os.makedirs('dataset', exist_ok = True)
    
    
    if not exists("dataset/redditTitles.csv"):
        redditApi = redditAPI()
        dfTitles = redditApi.getTopSubredditTitles("stocks", redditTimeFilter.WEEK.value)
        dfTitles.to_csv("dataset/redditTitles.csv", index = False)
    else:
        print("Reddit Titles dataset already exists")
        
        
    if not exists("dataset/redditComments.csv"):
        redditApi = redditAPI()
        dfTitles = pd.read_csv("dataset/redditTitles.csv")
        dfAllComments = pd.DataFrame()
        
        for index, redditTitle in dfTitles.iterrows():
           dfAllComments = dfAllComments.append(redditApi.getPostComments(redditTitle['postId'], redditTitle['subredditName']))

        dfAllComments.to_csv("dataset/redditComments.csv", index = False)
    else:
        print("Reddit Comments dataset already exists")
    
    

    
    
if __name__ == '__main__':
    main()
    