# -*- coding: utf-8 -*-
"""
Created on Sat Aug 27 14:24:58 2022

@author: minaguchi_kohei
"""

import tweepy
import time
import os

def get_api():
    #API_KEYとかはWEB上に公開すると無効になるので、環境変数に登録して使う
    
    API_KEY = "eFmziAfedjSvreCNyQacMfRJh"
    API_SECRET = "mLzk1MeSyksDYVfndMWHGzd70oRmBFkbAvbgFSlPHn4GJ90eT6"
    ACCESS_TOKEN = "1416565070134345728-wB0aYK24FE5Ao7cUrDkSdzw25dr8PK"
    ACCESS_TOKEN_SECRET = "qDZOmoLSYwzR6aw9avjjuTbcmjSGjqQYUgwUGL2aEvDOt"

    auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api

def tw_search(api,keyword):
    #RTを除外して検索
    q = f"{keyword} exclude:retweets"
    #無料版スタンダードAPIは1秒で50ツイートを取得できる
    #result_typeは取得するツイートをrecentとpopularityの両観点から選択
    tweets = api.search_tweets(q=q, count=5 ,result_type="mixed", tweet_mode='extended')
    
    tw_ids = []
    for tweet in tweets:
        user_id: str = tweet.user.screen_name
        tw_id: str = str(tweet.id)
        tw_date = tweet.created_at
        favo: int = tweet.favorite_count
        text: str = tweet.full_text.replace('\n','')
        
        if keyword in text:
            #取得ツイート確認
            print(user_id,tw_date,text)
            #リクエスト過多を防ぐためのsleep
        time.sleep(1)
            

def main():
    api = get_api()
    # 「#魔女兵器」はRTもいいねもする
    keyword = "#魔女兵器"
    tw_search(api,keyword)
    #for retw_id in retw_ids:
    #    tw_retweet(api,retw_id)
    #    tw_favo(api,retw_id)
    
    # 「魔女兵器」はなぜか完全一致検索にならず関係ないツイートもひっかかるので、いいねのみ
    keyword = "魔女兵器"
    tw_search(api,keyword)
    #for favotw_id in favotw_ids:
    #    tw_favo(api,favotw_id)
    
if __name__ == "__main__":
    main()