
import requests
from .settings import settings
import hmac
import hashlib
import base64

# Webhookから来たリクエストの署名の検証
def validate(environ, body):
    xsignature = environ["HTTP_X_LINE_SIGNATURE"].encode('utf-8')
    channel_secret = settings["line"]["channel_secret"].encode('utf-8')
    digest = hmac.new(channel_secret, body, hashlib.sha256).digest()
    base64_digest = base64.b64encode(digest)
    return xsignature == base64_digest


def reply(data):
    url = 'https://api.line.me/v2/bot/message/reply'
    requests.post(url, json={
        "replyToken": data["events"][0]["replyToken"],
        "messages": [{
            'type': 'text',
            'text': data["events"][0]["message"]["text"] + 'ほげほげ',
        }]
    }, headers={
        'content-type': 'application/json',
        'authorization': f'Bearer {settings["line"]["access_token"]}'
    })