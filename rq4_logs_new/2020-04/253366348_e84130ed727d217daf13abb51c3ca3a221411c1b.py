import os
import json
import time
from datetime import datetime, timedelta, timezone
import logging
import slack
from requests_oauthlib import OAuth1Session
import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from requests.exceptions import Timeout

JST = timezone(timedelta(hours=+9), "JST")

table_storage_name = os.getenv("TABLE_STORAGE_NAME")
table_storage_key = os.getenv("TABLE_STORAGE_KEY")
table_service = TableService(account_name=table_storage_name, account_key=table_storage_key)

twitter_ck = os.getenv("TWITTER_CK")
twitter_cs = os.getenv("TWITTER_CS")
twitter_at = os.getenv("TWITTER_AT")
twitter_as = os.getenv("TWITTER_AS")
twitter = OAuth1Session(twitter_ck, twitter_cs, twitter_at, twitter_as)

slack_token = os.getenv("SLACK_TOKEN")
slack_channel_id = os.getenv("SLACK_CHANNEL_ID")
client = slack.WebClient(slack_token)

twitter_search_since = (datetime.now(JST) - timedelta(minutes=5)).strftime("%Y-%m-%d_%H:%M:%S_JST")
twitter_search_until = datetime.now(JST).strftime("%Y-%m-%d_%H:%M:%S_JST")

def twitter_search(keywords):
    global twitter_search_since
    global twitter_search_until

    query_str = keywords + " " \
        "-RT " \
        "since:" + twitter_search_since + " " \
        "until:" + twitter_search_until

    params = {
        "q": query_str,
        "count": 5,
        "lang": "ja",
        "result_type": "recent",
        "modules": "status"
    }

    twitter_search_url = "https://api.twitter.com/1.1/search/tweets.json"
    logging.debug('>>> Twitter search start! - %s', params)
    for i in range(3):
        try:
            req = twitter.get(twitter_search_url, params = params, timeout=(3.0, 5.0))
        except Exception as e:
            logging.error('twitter search exception: %s', e)
        else:
            break
    else:
        logging.error('twitter search failed: %s', query_str)
        return []
    logging.debug('<<< Wwitter search stop!')

    if req.status_code == 200:
        tweets = json.loads(req.text)['statuses']
        return tweets
    else:
        logging.error('twitter search failed.')
        return []

def tweet_filter_by_user(tweets, users):
    for user in users:
        for i, tweet in enumerate(tweets):
            if tweet['user']['screen_name'] == user['RowKey']:
                del tweets[i]
    return tweets

def get_permalink_by_tweet(tweets):
    permalinks = []
    for tweet in tweets:
        permalinks.append('https://twitter.com/' + tweet['user']['screen_name'] + '/status/' + tweet['id_str'])
    return permalinks

def main(myeventtimer: func.TimerRequest) -> None:
    logging.info('twitter ego-searching function v0.4')
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if myeventtimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    global twitter_search_since
    global twitter_search_until
    twitter_search_until = datetime.now(JST).strftime("%Y-%m-%d_%H:%M:%S_JST")
    logging.info('time range to Twitter search: since %s, until %s',
                    twitter_search_since, twitter_search_until)

    permalinks = []
    query_keywords = table_service.query_entities('TwitterEgoSearch', filter="PartitionKey eq 'Keyword'")
    filtering_users = table_service.query_entities('TwitterEgoSearch', filter="PartitionKey eq 'FilterUser'")
    for keyword in query_keywords:
        tweets = twitter_search(keyword['RowKey'])
        tweets = tweet_filter_by_user(tweets, filtering_users)
        permalinks.extend(get_permalink_by_tweet(tweets))

    twitter_search_since = twitter_search_until

    for url in set(permalinks):
        logging.info('Permalink: %s', url)
        client.chat_postMessage(channel=slack_channel_id, text=url)