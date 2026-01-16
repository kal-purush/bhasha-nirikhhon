import os
from operator import getitem
from . import util
from pprint import pprint
import hmac
import hashlib
from datetime import datetime
import time
import json
import base64
import requests
import urllib
from bson.objectid import ObjectId
from urllib.parse import urlencode, quote, quote_plus, parse_qs, unquote
from . import twitter
if __name__ != '__main__':
    from .settings import settings
    from . import util
    from .util import Just
    from .db import get_collection
    from .import certificate
    users_db = get_collection('users')
    TOKENS_FILE = 'kyukou/tokens'
else:
    import util
    TOKENS_FILE = 'tokens'


def generate_signature(method, raw_url, raw_params, raw_key):
    param_string = quote(urlencode(sorted(raw_params.items(), key=lambda e: e[0])), safe='')
    url = quote(raw_url, safe='')
    signature_base_string = '&'.join([method, url, param_string]).encode()
    signing_key = raw_key.encode()
    digest = hmac.new(signing_key, signature_base_string, hashlib.sha1).digest()
    return base64.b64encode(digest)


def tweet_basic(msg, oauth_token, oauth_token_secret):
    url = f'https://api.twitter.com/1.1/statuses/update.json'
    res = post(url, oauth_token, oauth_token_secret, {
        'status': msg
    })
    if res.status_code != 200:
        print('tweet', res.status_code, res.text)
    return res


def tweet(msg):
    tokens = load_tokens()
    return tweet_basic(msg, tokens['oauth_token'], tokens['oauth_token_secret'])


def get_access_token(oauth_token, oauth_verifier):
    url = f'https://api.twitter.com/oauth/access_token?oauth_verifier={oauth_verifier}'
    tokens = load_tokens()
    res = post(url, oauth_token, tokens['oauth_token_secret'])
    if res.status_code == 200:
        return parse_query(res.text)
    else:
        return None


def parse_query(src):
    try:
        r = {}
        for e in src.split('&'):
            s = e.split('=')
            r[unquote(s[0])] = unquote(s[1])
        return r
    except:
        return None


def post(baseurl, oauth_token, oauth_token_secret, d={}, nonce='hogehogehoge', headers={}, data=''):
    url = f'{baseurl}{"?"if d else ""}{urlencode(d)}'
    raw_params = {
        'oauth_callback': settings.url_prefix()+settings.twitter.callback_path(),
        "oauth_consumer_key": settings.twitter.consumer_key(),
        "oauth_signature_method": 'HMAC-SHA1',
        "oauth_timestamp": time.time(),
        "oauth_nonce": nonce,
        "oauth_version": "1.0",
        "oauth_token": oauth_token
    }
    params = dict(raw_params)
    for k, v in d.items():
        params[k] = v
    raw_key = settings.twitter.consumer_key_secret()+'&'+oauth_token_secret
    raw_params['oauth_signature'] = generate_signature('POST', baseurl, params, raw_key)
    default_headers = {
        'Authorization': f'OAuth {urlencode(raw_params).replace("&",",")}'
    }
    default_headers.update(headers)
    return requests.post(url, headers=default_headers, data=data)


def store_tokens(o):
    with open(TOKENS_FILE, 'wt', encoding='utf-8') as f:
        json.dump(o, f)


def load_tokens():
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, 'rt', encoding='utf-8') as f:
            return json.load(f)
    else:
        return None


def get_redirect_url():
    raw_url = 'https://api.twitter.com/oauth/request_token'
    res = post(raw_url, settings.twitter.access_token(), settings.twitter.access_token_secret())
    print(res.text)
    query = parse_query(res.text)
    store_tokens(query)
    return 'https://api.twitter.com/oauth/authenticate?oauth_token='+query['oauth_token']


def register(user_id, data, realid=None):
    if realid:
        users_db.update_one({'_id': ObjectId(realid)}, {
            '$set': {
                'connections.twitter': data
            }
        })
    elif users_db.find_one({'connections.twitter.id': user_id}):
        users_db.update_one({'connections.twitter.id': user_id}, {
            '$set': {
                'connections.twitter': data
            }
        })
    else:
        users_db.insert_one({
            'connections': {
                'twitter': data
            },
        })


def get_real_user_id(user_id):
    return Just(users_db.find_one({"connections.twitter.id": user_id}))._id[lambda e:None if e == None else str(e)]()


def get_twitter_user_id(real_user_id):
    data = Just(users_db.find_one({"_id": ObjectId(real_user_id)}))
    if data() and 'twitter' in data.connections():
        return data.connections.twitter.user_id()
    else:
        raise RuntimeError


def parse(o):
    data = Just(o)
    # direct messsage event
    event = data.direct_message_events[lambda l:l[0]]
    if event():
        user_id = event.message_create.sender_id()
        if user_id != load_tokens()['user_id']:
            msg_text = event.message_create.message_data.text()
            if not get_real_user_id(user_id):
                register(user_id, data.users[user_id])
            twitter.direct_message(user_id, msg_text)
            return
        else:
            return
    # follow event
    event = data.follow_events[lambda l:l[0]]
    if event():
        register(event.source.id(), event.source())
        twitter.follow(user_id)
        return
    # dm indicate typing event
    event = data.direct_message_indicate_typing_events[lambda l:l[0]]
    if event():
        return
    # tweet delete event
    event = data.tweet_delete_events[lambda l:l[0]]
    if event():
        return
    # favorite event
    event = data.favorite_events[lambda l:l[0]]
    if event():
        return
    # tweet create event
    event = data.tweet_create_events[lambda l:l[0]]
    if event():
        return

    print(o)


def send(user_id, msg_text):
    url = 'https://api.twitter.com/1.1/direct_messages/events/new.json'
    data = {
        'event': {
            'type': 'message_create',
            'message_create': {
                'target': {
                    'recipient_id': user_id,
                },
                'message_data': {
                    'text': msg_text,
                }
            }
        }
    }
    tokens = load_tokens()
    res = post(url, tokens['oauth_token'], tokens['oauth_token_secret'], headers={
        'content-type': 'application/json'
    }, data=json.dumps(data))
    if res.status_code != 200:
        print('send', res.status_code, res.text)
    return res


def validate_webhook():
    url = f'https://api.twitter.com/1.1/account_activity/all/{settings.twitter.account_activity_api_env()}/webhooks.json'  # ?url={settings.url_prefix()}{settings.twitter.callback_path()}'
    return post(url, settings.twitter.access_token(), settings.twitter.access_token_secret(), {
        'url': f'{settings.url_prefix()}{settings.twitter.webhook_path()}'
    })


def subscribe_user(webhook_id, oauth_token, oauth_token_secret):
    url = f'https://api.twitter.com/1.1/account_activity/all/{settings.twitter.account_activity_api_env()}/subscriptions.json'
    res = post(url, oauth_token, oauth_token_secret, headers={
        'content-type': 'application/json'
    }, data=json.dumps({
        'webhook_id': webhook_id
    }))
    print(res.status_code, res.status_code == 204)


tweet('こころぴょんぴょん')