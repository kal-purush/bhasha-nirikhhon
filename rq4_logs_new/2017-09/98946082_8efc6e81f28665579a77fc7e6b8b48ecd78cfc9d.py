#!/usr/bin/env python
# -*- coding:utf-8 -*-

import urllib
import urllib2
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
from flask import Flask,request
import json


app = Flask(__name__)

@app.route('/matter', methods=['POST'])
def main():
    data = json.loads(dict(request.form.lists())["payload"][0])
    if "action" in data:
        body = data["comment"]["body"].encode('utf-8')
        # メンション付きか判定
        if "@" in body:
            # 宛先を特定
            if "@ManaSAITO" in body:
                user = "@mana.test"
            elif "@test" in body:
                user = "@test"
            else:
                pass
        issue_num = data["issue"]["number"]
        issue_url = data["issue"]["html_url"].encode('utf-8')
        title = data["issue"]["title"].encode('utf-8')
        text =  user + ' new comments to you on' + ' #' + str(issue_num) + title + '\n' + issue_url + '\r\n>' + body
        

        # chat.postMessageのURL
        url = 'https://slack.com/api/chat.postMessage'
        token = ''
        user = '@mana.test'

        # パラメータ
        params = {
            # slack上で取得したAPIトークン
            'token' : token,
            # 投稿先チャンネル
            'channel' : user,
            # 投稿メッセージ
            'text' : text
        }
        # クエリ文字列に変換
        params = urllib.urlencode(params)
        params = params.encode('utf-8')
        # リクエスト生成
        req = urllib2.Request(url, params)
        # ヘッダ追加
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        # URLを開く
        res = urllib2.urlopen(req)
        # レスポンス取得
        body = res.read()
        # レスポンスを表示
        print (body.decode('utf-8'))

    else:
        return 'false'
    return 'success'
if __name__ == '__main__':
    app.debug = True
    app.run()    