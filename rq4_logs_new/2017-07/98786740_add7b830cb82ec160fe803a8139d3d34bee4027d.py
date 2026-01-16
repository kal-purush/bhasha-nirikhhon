# -*- coding: utf-8 -*-

from flask import Flask, render_template, request
from wxpy import *

import logging
LOG_FILENAME="log.txt"
logging.basicConfig(filename=LOG_FILENAME,level=logging.WARNING)

app = Flask(__name__)
bot = Bot()

def checkAndSend(to, content):
    if to=="" or content=="":
        return "to和content都得填."
    logging.warning("wechat msg to:{} content:{}".format(to, content))
    my_friend = bot.friends().search(to)[0]
    my_friend.send(content)
    return "OK"

@bot.register()
def auto_reply(msg):
    return 'i am alive..'

@bot.register(msg_types=FRIENDS)
def auto_accept_friends(msg):
    new_friend = msg.card.accept()
    new_friend.send('hi, 这是个用来发消息的机器人, 使用方法: https://github.com/astwyg/wxrobot')

@app.route('/msg',  methods=['GET','POST'])
def msg():
    if request.method == "GET":
        result = checkAndSend(request.args.get("to",""), request.args.get("content",""))
    elif request.method == "POST":
        result = checkAndSend(request.form.get("to",""), request.form.get("content",""))
    else:
        result = "用GET或者POST方法, 你弄了个{}".format(request.method)
    return result

if __name__ == '__main__':
    app.run(debug=False, port=5003, host="0.0.0.0")