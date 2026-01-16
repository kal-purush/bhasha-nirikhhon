# The challenge is to build a HTTP-based RESTful API for managing Short URLs and redirecting clients similar to bit.ly or goo.gl. Be thoughtful that the system must eventually support millions of short urls. Please include a README with documentation on how to build, and run and test the system. Clearly state all assumptions and design decisions in the README.
# A Short Url:
#
# 1. Has one long url
#
# 2. Permanent; Once created
#
# 3. Is Unique; If a long url is added twice it should result in two different short urls.
#
# 4. Not easily discoverable; incrementing an already existing short url should have a low probability of finding a working short url.
#
# Your solution must support:
#
# 1. Generating a short url from a long url
#
# 2. Redirecting a short url to a long url within 10 ms.
#
# 3. Listing the number of times a short url has been accessed in the last 24 hours, past week and all time.
#
# 4. Persistence (data must survive computer restarts)
#
# Shortcuts
#
# 1. No authentication is required
#
# 2. No html, web UI is required
#
# 3. Transport/Serialization format is your choice, but the solution should be testable via curl

from flask import Flask, json, jsonify, request, g, make_response
from sqlitedict import SqliteDict
import webbrowser
from hashlib import (md5)
from base62 import b62encode

def encodeUrl(url):
    return b64encode(md5(url.encode()).digest())

api = Flask(__name__)
LONG_TO_SHORT_FILE = 'long2short.sqlite'
SHORT_TO_LONG_FILE = 'short2long.sqlite'

long2short = SqliteDict(LONG_TO_SHORT_FILE)
short2long = SqliteDict(SHORT_TO_LONG_FILE)

@api.route('/encode', methods=['POST'])
def encode():
    data = request.get_json()
    print(data['longUrl'])
    url = data['longUrl']

    if not isinstance(url, str):
        resp = make_response("Invalid URL", 400)
        return resp

    # if url not in long2short:
    long2short[url] = encodeUrl(url)

    short_url = long2short[url]
    short2long[short_url] = url
    print(short_url, type(short_url), str(short_url))
    #return jsonify({"shortUrl": short_url})


@api.route('/decode', methods=['GET'])
def decode():
    data = request.get_json()
    url = data['shortUrl']

    if not isinstance(url, str):
        resp = make_response("Invalid URL", 400)
        return resp

    if url not in short2long:
        resp = make_response("URL not found", 404)
        return resp

    # url = 'https://codefather.tech/blog/'
    # webbrowser.open(url)
    return jsonify({"longUrl": short2long[url]})

@api.route('/stats', methods=['GET'])
def stats(url):
    return json.dumps()


if __name__ == '__main__':

    api.run()