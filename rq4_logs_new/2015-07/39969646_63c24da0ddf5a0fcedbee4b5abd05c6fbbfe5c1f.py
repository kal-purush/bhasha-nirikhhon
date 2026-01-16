#!/usr/bin/python
# -*- coding: utf-8 -*-

import json, tweepy, sys, argparse, os
import urllib, urllib2, datetime
from config import *
from pack import future, calcworker, textworker, feedworker

tw_api = None

def init_twitter_api():
    auth = tweepy.OAuthHandler(C_KEY, C_SECRET)  
    auth.set_access_token(A_TOKEN, A_TOKEN_SECRET)  
    return tweepy.API(auth)

def make_bitlink(url):
    params = {
        "access_token" : BITLY_ACCESS_TOKEN,
        "domain"       : "j.mp",
        "longUrl"      : url,
        "format"       : "json"
    }
    query_string = urllib.urlencode(params)
    try:
        http_resp = urllib2.urlopen(BITLY_API_URL + "/v3/shorten?" + query_string)
    except urllib2.URLError as e:
        print "Connection error: " + str(e.reason)
        return None
    except urllib2.HTTPError as e:
        print "Request failed with status: " + str(e.code)
        return None
    else:
        text_resp = http_resp.read()
        parsed_resp = json.loads(text_resp)
        bitlink = parsed_resp["data"]["url"].replace("\\", "")
        return bitlink 

def init_cli():
    parser = argparse.ArgumentParser(description="Twitter Bot Script")
    parser.add_argument("--new-status", nargs=1, help="enter text to post in twitter", type=str)
    parser.add_argument("--rank-influencers", action="store_true", 
        help="create or update rank scores for twitter influencers")
    parser.add_argument("--post-article", action="store_true", 
        help="post link to article from tracked rss/atom feeds in twitter")
    args = parser.parse_args()
    return vars(args)

def post_tweet(text):
    if len(text) == 0 or len(text) > 140:
        print "Length of tweet has to be between 1 and 140 characters"
        return None
    try:
        tw_api.update_status(status=text)
    except tweepy.error.TweepError as e:
        print "Status Update Failed!"
        print e
        return None
    return True
       
def get_user_statuses(screen_name, trim_user=False, count=20):
    try:
        statuses = tw_api.user_timeline(screen_name=screen_name, count=count, 
            trim_user=trim_user, include_entities=False)
    except tweepy.error.TweepError as e:
        print e
        return []
    else:
        return statuses

def query_user_profile_info(username):
    try:
        user_info = tw_api.get_user(screen_name=username, include_entities=False)
    except TweepError as e:
        print e
        return false 
    else:
        return user_info

def get_user_info(username):
    user_info = {}
    statuses = get_user_statuses(username, False, 50)
    if len(statuses) == 0: return false
    # every status object contains user info 
    user_info["followers_count"] = statuses[0].user.followers_count 
    user_info["list_count"] = statuses[0].user.listed_count 
    user_info["screen_name"] = statuses[0].user.screen_name
    count = 0
    for status in statuses:
        count += status.retweet_count
    user_info["retweet_count"] = count
    return user_info
            
def rank_influencers(usernames):
    global future, calcworker
    futures = [future.Future(get_user_info, username) for username in usernames]
    users_info = [future() for future in futures] 
    users_ranks = calcworker.get_users_ranks(users_info)
    return users_ranks

def create_update_ranks_db(usernames):
    users_ranks = rank_influencers(usernames)
    file = open(RANKS_DB, "w")
    for screen_name in users_ranks:
        file.write("%s | %f \n" % (screen_name, users_ranks[screen_name]))
    file.close()

def process_user_statuses(screen_name, rank, keywords):
    statuses_objs = get_user_statuses(screen_name, True, 100)
    statuses_texts = [status.text.encode('utf-8') for status in statuses_objs] 
    pure_texts = textworker.purify_texts(statuses_texts)
    concat_text = textworker.concat_texts(pure_texts)
    words_frequency = textworker.count_word_frequency(concat_text, keywords)
    words_local_scores = calcworker.get_words_local_scores(words_frequency, rank) 
    return words_local_scores

def digest_feeds_entries(feed_url, keywords, posted_titles, words_scores):
    all_feed_entries = feedworker.process_feed(feed_url)
    # exclude published titles
    feed_entries = [entry for entry in all_feed_entries if entry["title"] not in posted_titles]
    if len(feed_entries) == 0: return None
    # here each entry becomes element of a list
    entries_content = [entry["content"] for entry in feed_entries]
    # which I can pass to function
    pure_entries = textworker.purify_texts(entries_content, True, True)
    # turn each entry into a string 
    pure_entries = [" ".join(entry) for entry in pure_entries]
    feed_records = [] 
    for i in range(0, len(pure_entries)):
        # match_count and match_words are necessary only for debugging
        entry_score, match_count, matched_words = calcworker.get_document_score(textworker.count_word_frequency(pure_entries[i], keywords), words_scores)
        feed_records.append({"title": feed_entries[i]["title"], "link":
            feed_entries[i]["link"], "score": entry_score, "match_count": match_count, "matched_words": matched_words})
    return feed_records 

def prepare_new_status(title, link, intro_text):
    bitlink = make_bitlink(link)
    if not bitlink:
        print "Couldn't create a short link."
        print "Operation stopped."
    title_length_allowed = 140 - len(intro_text) - len(bitlink) - 1
    post_title = title 
    if len(post_title) > title_length_allowed:
        post_title = title[0:title_length_allowed-4] + "..."
    new_status = intro_text.encode('utf-8') + post_title + " " + bitlink.encode('utf-8')
    return new_status

def exec_commands(args, future):
    for c in args:
        if (c == "new_status" and isinstance(args[c], list) and  
            isinstance(args[c][0], str) and len(args[c][0]) > 0):
            post_tweet(args[c][0])
        if c == "rank_influencers" and args[c]:
            try:
                config_file = open(CONTENT_CONFIG_PATH, "r")
            except IOError as e:
                print ("Content config file doesn't exists at provided location:" 
                        + CONTENT_CONFIG_PATH)
                exit(0)
            else:
                try:
                    usernames = json.loads(config_file.read())["twitter_influencers"]
                except ValueError as e:
                    print "JSON file " + CONTENT_CONFIG_PATH + " is malformed!"
                    print e
                except KeyError as e:
                    print "Key " + str(e) + " is not present in JSON file"
                else:
                    create_update_ranks_db(usernames)
                config_file.close()
        if c == "post_article" and args[c]:
            try:
                config_file = open(CONTENT_CONFIG_PATH, "r")
            except IOError as e:
                print "File " + CONTENT_CONFIG_PATH + " does not exists." 
            else:
                file_content = config_file.read()
                keywords = json.loads(file_content)["keywords"] 
                if len(keywords) == 0:
                    print ("List of keywords in " + CONTENT_CONFIG_PATH + " is empty") 
                    exit(0)
                keywords = textworker.extend_keywords_set_with_corpora(keywords)
                feeds_urls  = json.loads(file_content)["feeds"]
                if len(feeds_urls) == 0:
                    print ("List of feeds in " + CONTENT_CONFIG_PATH + " is empty") 
                    exit(0)
                intro_text = json.loads(file_content)["intro_text"]
                config_file.close()
            try:
                ranks_db = open(RANKS_DB, "r")
            except IOError as e:
                print "File " + RANKS_DB + " does not exist."
                print "Run <bot.py --rank-influencers> command to create it."
            else:
                # twitter processing
                inf_ranks = []
                for line in ranks_db:
                    rank = line.split(" | ")
                    inf_ranks.append({"screen_name": rank[0], "rank": float(rank[1])})
                ranks_db.close()
                futures = [future.Future(process_user_statuses, rank_obj["screen_name"], 
                        rank_obj["rank"], keywords) for rank_obj in inf_ranks]
                local_keywords_scores = [task() for task in futures] 
                global_keywords_scores = calcworker.get_words_global_scores(local_keywords_scores)
                if len(global_keywords_scores) == 0: 
                    print "Something went wrong. List of keywords wasn't generated."
                    exit(1);
                # sites feeds processing
                posted_titles = []
                if os.path.exists(FEEDS_DB):
                    feeds_db = open(FEEDS_DB, "a+")
                    for line in feeds_db:
                        posted_titles.append(line.split(" ; ")[0])
                else:
                    feeds_db = open(FEEDS_DB, "w")
                futures = [future.Future(digest_feeds_entries, feed_url,
                    keywords, posted_titles, global_keywords_scores) for feed_url in feeds_urls]
                # each list element itself a list
                feeds_posts = [task() for task in futures]
                # flatten the list
                all_posts = []
                for feed_posts in feeds_posts:
                    if type(feed_posts) == type(None): continue 
                    for post in feed_posts:
                        all_posts.append(post)
                if len(all_posts) == 0:
                    print "Something went wrong. There are no articles to post!"
                else:
                    best_post = None
                    max_score = 0    
                    for post in all_posts:
                        if post["score"] > max_score:
                            best_post = post
                            max_score = post["score"]
                    # fallback for the case when articles didn't contain
                    # tracked keywords
                    if not best_post and len(all_posts) > 0:
                        best_post = all_posts[0]
                    new_status = prepare_new_status(best_post["title"], best_post["link"], intro_text)
                    if not post_tweet(new_status):
                        print "Something went wrong. New status wasn't posted."
                    else:
                        feeds_db.write(best_post["title"] + " ; \n")
                feeds_db.close()

def main(argv):
    global tw_api
    tw_api = init_twitter_api()
    cli_args = init_cli()
    exec_commands(cli_args, future)

if __name__ == "__main__":
    main(sys.argv)