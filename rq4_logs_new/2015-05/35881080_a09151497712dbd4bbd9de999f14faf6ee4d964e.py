__author__ = 'shreyas'
import twitter
import time
from datetime import datetime
import math
import pytz
from nltk.corpus import stopwords

api = twitter.Api(
    consumer_key='*****',
	consumer_secret='*****',
	access_token_key='*****',
	access_token_secret='*****',
    )
companies = ['aapl','msft','goog','crm']
stopwords=set(stopwords.words('english'))
sep = '|'
fm = open('metadata.txt','w')
ft = open('tweets.txt','w')
fw = open('words.txt','w')
count = 2000
query_count= 1
query_rate_limit=180
query_limit_interval = 60*15 + 30
mline =  'company'+sep+'tweet_id'+sep+'user_screen_name'+sep+'user_followers_count'+sep+'is_user_verified'+sep+'user_location'+sep+'created_at'+sep+'coordinates'+sep+'is_retweeted'+sep+'retweet_count'+sep+'is_favorited'+sep+'favorite_count'+sep+'geo'+sep+'location'+'\n'
tline = 'id'+sep+'tweet_text'+'\n'
wline = 'id'+sep+'tweet_word'+sep+'tfidf'+'\n'
fm.write(mline)
ft.write(tline)
fw.write(wline)
termdict ={}
#time.sleep(query_limit_interval)
for company in companies:
    i = 0;
    max_id = -1
    while i < count:
        if query_count == query_rate_limit:
            print(time.localtime())
            print ('slept')
            time.sleep(query_limit_interval)
            print(time.localtime())
            print ('woke up')
            query_count = 1
            print (count)
            print(i)
            print(query_count)
            print('\n')
        if max_id < 0:
            search = api.GetSearch(term=company, lang='en', count=100, max_id='')
        else:
            search = api.GetSearch(term=company, lang='en', count=100, max_id=str(max_id))
        query_count += 1
        for t in search:
            createdat=datetime.strptime(str(t.created_at),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
            mline =  (company+sep+str(t._id)+sep+str(t.user.screen_name).replace("|", "/")+sep+str(t._user._followers_count).replace("|", "/")+sep+str(t._user._verified).replace("|", "/")+sep+str(t._user._location.encode('utf-8')).replace("|", "/") + sep +str(createdat)+ sep+str(t._coordinates).replace("|", "/")+ sep+str(t._retweeted)+ sep+str(t._retweet_count)+ sep+str(t._favorited)+ sep+str(t._favorite_count)+ sep+str(t._geo).replace("|", "/")+ sep+str(t.location).replace("|", "/")+'\n')
            text = str(t.text.encode('utf-8'))
            text = text.replace("\r\n", " ")
            text = text.replace("\n", " ")
            text = text.replace("|", "/")
            text = text.lower()
            tweet_id = str(t._id)
            if tweet_id==' ':
                continue
            tline =  (tweet_id+ sep+text+'\n')
            words = filter(lambda w: not w in stopwords,text.split(" "))
            termdict[tweet_id]=words
            freq = {}
            for word in words:
                if word.isalpha():
                    if freq.has_key(word):
                        freq[word] += 1
                    else:
                        freq[word] = 1
            termdict[tweet_id] = freq

            fm.write(mline)
            ft.write(tline)
            max_id = t.id
            i += 1
fm.flush()
fm.close()
ft.flush()
ft.close()

#TFIDF ALGORITHM
doc_count = len(termdict)
for tweet_id in termdict:
    words = termdict[tweet_id]
    for word in words:
        word_df = 0
        if word == "is":
            word_df += 0

        for i_tweet_id in termdict:
            i_words = termdict[i_tweet_id]
            if word in i_words:
                word_df += 1

        #tf = math.log(1+words[word])
        #idf = math.log(doc_count/(word_df))
        tf = words[word]
        idf = doc_count/word_df
        tf_idf = str(tf*idf)
        wline =  (tweet_id+ sep+word+sep+tf_idf+'\n')
        fw.write(wline)

fw.flush()
fw.close()
