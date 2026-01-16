import tweepy
import time
import os
import tw_db

def get_api():
    
    API_KEY = os.environ.get('API_KEY')
    API_SECRET = os.environ.get('API_SECRET')
    ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET')

    auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api

def tw_search(api,keyword):
    q = f"{keyword} exclude:retweets"
    tweets = api.search_tweets(q=q, count=50 ,result_type="mixed", tweet_mode='extended')
    
    tw_ids = []
    for tweet in tweets:
        user_id: str = tweet.user.screen_name
        tw_id: str = str(tweet.id)
        tw_date = tweet.created_at
        favo: int = tweet.favorite_count
        text: str = tweet.full_text.replace('\n','')
        print(user_id,tw_date,text)
        time.sleep(1)
        
        if tw_db.check_db(tw_id) == True:
            print("This tweet has already been registered")
            continue
        else:
            tw_ids.append(tw_id)
            tw_db.insert_db(tw_id)
        
    if tw_db.count_db()>10:
        tw_db.del_900()
    return tw_ids

def tw_retweet(api,tw_id):
    try:
        api.retweet(tw_id)
    except:
        print("retweet error")

def tw_favo(api,tw_id):
    try:    
        api.create_favorite(id=tw_id)
    except:
        print("favorite error")
     
def main():
    api = get_api()
    keyword = "#魔女兵器"
    retw_ids = tw_search(api,keyword)
    #for retw_id in retw_ids:
    #    tw_retweet(api,retw_id)
    
    keyword = "魔女兵器"
    favotw_ids = tw_search(api,keyword)
    #for favotw_id in favotw_ids:
    #    tw_favo(api,favotw_id)
    
if __name__ == "__main__":
    main()