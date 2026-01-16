'''
Created on 2015-08-05

@author: Niranjan    
'''

from django.utils.encoding import smart_str
from couchdb.client import Server
from TwitterAPI import TwitterAPI
import time
import math
import json
import sys
from twython import Twython

program_no = 'frndsk1'

#Karthik's keys
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_KEY=""
ACCESS_SECRET=""

api = TwitterAPI(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_KEY,ACCESS_SECRET)

def get_new(screen_name):
    temp_dict=dict()
    followers=dict()
    l1=['friends']
    twitter = Twython(CONSUMER_KEY , CONSUMER_SECRET,ACCESS_KEY, ACCESS_SECRET)
    user = screen_name
    fnum = 5000
    try:
        check_rate_limit_status('application/rate_limit_status')
    except Exception as e:
        print e
        print 'sleeping for 15 mins...'
        time.sleep(900)
    
    try:
        suser = twitter.show_user(screen_name=user)
        temp_dict['_id']=smart_str(suser['id'])
        temp_dict['id']=smart_str(suser['id'])
        temp_dict['screen_name']=smart_str(suser['screen_name'])
        for typ in l1:
            dname=dict()
            pnum = int(math.ceil(float(suser[str(typ)+"_count"]) / fnum))
            pages = []
            for i in range(pnum):
                pages.append("p"+str(i+1))
            oldpages = []
            for i in range(pnum):
                oldpages.append("p"+str(i))
            
            p0 = { "next_cursor": -1 } # So the following exec() call doesn't fail.
            
            for i in range(pnum):
                check_rate_limit_status1(typ+'/ids',typ)
#                 exec(pages[i]+" = twitter.get_"+str(typ)+"_ids(screen_name=user, count=fnum, skip_status=1, cursor="+oldpages[i]+"['next_cursor'])")
                exec(pages[i]+" = twitter.get_"+str(typ)+"_ids(screen_name=user, count=fnum, cursor="+oldpages[i]+"['next_cursor'])")
#                 ps=twitter.get_friends_list(screen_name=user)
#                 print ps
#                 sys.exit(1)
#             sys.exit(1)
            followers = []
            for p in range(pnum):
                try:
                    exec("for i in range(fnum): followers.append("+pages[p]+"['ids'][i])")
                except(IndexError):
                    pass
            temp_dict[str(typ)]=followers
            print'No. of %s:%d'%(typ,len(followers))
    except:
        print screen_name
        return 0
    return temp_dict
  
def Create_Couchdb_Instance(dtbs_name):
    
    server = Server("http://141.217.48.222:5984")
    try:
        db = server.create(dtbs_name)
    except Exception:
        db = server[dtbs_name]
    return db  
    
def check_rate_limit_status(lim_type):
    
    api.request('application/rate_limit_status')
    iter1 = api.get_iterator()
    for item in iter1:
        tweet_query_limit = item['resources']['statuses']['/statuses/show/:id']['remaining']
        print tweet_query_limit
    if tweet_query_limit <= 0 :
        print 'Rate limit reached .. sleeping for 15 mins ..'
        time.sleep(900)

def check_rate_limit_status1(lim_type,typ):
    
    api.request('application/rate_limit_status')
    iter1 = api.get_iterator()
    for item in iter1:
        tweet_query_limit = item['resources'][str(typ)]['/'+str(lim_type)]['remaining']
        print tweet_query_limit
    if tweet_query_limit <= 0 :
        print 'Rate limit reached .. sleeping for 15 mins ..'
        from datetime import datetime
        print datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        time.sleep(900)


def start_proc():
    s_cnt=0
    no_users=0
    no_errors=0
    no_notfound=0
    db = Create_Couchdb_Instance('twitter_frnds')
    dict_unames = json.loads(open('snames_All.json').read())
    for screen_name in dict_unames:
        s_cnt+=1
        try:
            if s_cnt>112500 and s_cnt<135000:
                print 'scr:%s'%(str(screen_name))
                temp_dict = get_new(screen_name)
                if temp_dict!=0:
                    db.create(temp_dict)
                    
                else:
                    no_notfound+=1
                no_users+=1
                write_log(no_users,no_errors,no_notfound)
        except Exception as e:
            print 'Error encountered : '
            print e, screen_name
            no_errors+=1

def write_log(no_users,no_errors,no_notfound):
    file = open('log_'+program_no+'.txt','w')
    file.write('No of Users Collected : '+smart_str(no_users)+'\n')
    file.write('No of errors : '+smart_str(no_errors))
    file.write('No of users not found:'+smart_str(no_notfound))
    file.close()         
            
if __name__ == '__main__':
#     get_followers()
    start_proc()
#     check_rate_limit_status1('friends/list','friends')
