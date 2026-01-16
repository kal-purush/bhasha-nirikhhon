import tweepy
import time
import os
import tw_db
import random 

def get_api():
    #API_KEYとかはWEB上に公開すると無効になるので、環境変数に登録して使う
    API_KEY = os.environ.get('API_KEY')
    API_SECRET = os.environ.get('API_SECRET')
    ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET')

    auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api

def tw_search(api,keyword):
    #RTを除外して検索
    query = f"{keyword} - RT"
    label = os.environ.get('DEV_LABEL')
    tweets = api.search_30_day(label, query, maxResults=10)
    
    tw_ids = []
    for tweet in tweets:
        user_id: str = tweet.user.screen_name
        tw_id: str = str(tweet.id)
        tw_date = tweet.created_at
        favo: int = tweet.favorite_count
        text: str = tweet.text.replace('\n','')
        
        #リクエスト過多を防ぐためのsleep
        time.sleep(1)
        #検索を完全一致に
        if "魔女兵器" in text:
            #取得したツイートがデータベースに登録済みならスルー
            if tw_db.check_db(tw_id) == True:
                print(f"{tw_id} has already been registered")
                continue
            #未登録ツイートの処理、データベースにも登録
            else:
                #取得ツイート確認
                print(user_id,tw_date,text)
                tw_ids.append(tw_id)

    #Heroku Postgres無料版はデータを10000行まで登録できる
    #余裕をもって1000行まで行ったら、前から900行分削除
    if tw_db.count_db()>1000:
        tw_db.del_900()
    return tw_ids

def tw_retweet(api,tw_id):
    try:
        api.retweet(tw_id)
        tw_db.insert_db(tw_id)
        time.sleep(30*60)
        return True
    except:
        print("retweet error")
        return False

def tw_favo(api,tw_id):
    try:    
        api.create_favorite(id=tw_id)
    except:
        print("favorite error")
     
def main():
    api = get_api()
    #検索キーワード
    keywords = ["魔女兵器"]
    
    #キーワード順でRTが偏らないようにキーワードをシャッフル
    random.shuffle(keywords)
    
    retw_cnt = 0
    keyword = keywords[0]
    retw_ids = tw_search(api,keyword)
    """
    for retw_id in retw_ids:
        if tw_retweet(api,retw_id):
            cnt+=1
            
        if cnt > 7:
            break
    if cnt > 7:
        break
    """   
            
            
if __name__ == "__main__":
    main()