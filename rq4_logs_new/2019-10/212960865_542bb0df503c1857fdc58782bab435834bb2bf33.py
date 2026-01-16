import urllib
import requests
import json
from 特殊牌型 import *
from 普通牌型 import *
from tkinter import *
from sort_cards import *
from itertools import *
import json

####本文件作为测试算法文件，

token = ""
id = ""
user_id = ""
cards = ""
def startgame():#开启战局
    global token,id,cards
    url = "https://api.shisanshui.rtxux.xyz/game/open"
    headers = {'x-auth-token': token}
    res = requests.post(url, headers=headers)
    info = res.json()
    if info['status'] == 0:
        id = info['data']['id']
        cards = info['data']['card']
        print('手牌:',cards)

def postcards():#出牌
    global token,id,cards
    url = "https://api.shisanshui.rtxux.xyz/game/submit"
    headers = {'content-type': "application/json"}
    headers['x-auth-token'] = token
    data = {'id':id}
    print('出牌牌型:')
    best_match = special_cards_type(cards)
    if best_match[0]:
        data['card'] = best_match[1]
    else:
        data['card'] = best_cards_type(cards)
    res = requests.post(url, data=json.dumps(data),headers=headers)
    info = res.json()
    #print(info)

def sign_up():#注册
    url = "https://api.shisanshui.rtxux.xyz/auth/register"
    headers = {'content-type': "application/json"}
    username = "lxc"
    password = "lxc"
    data = {
        "username":username,
        "password":password
    }
    res = requests.post(url,data=json.dumps(data),headers=headers)

def login():#登录
    global token,user_id
    url = "https://api.shisanshui.rtxux.xyz/auth/login"
    headers = {'content-type': "application/json"}
    username = "lxc"
    password = "lxc"
    data = {
        "username": username,
        "password": password
    }
    res = requests.post(url, data=json.dumps(data), headers=headers)
    info = res.json()
    #print(info)
    token = info['data']['token']
    user_id = info['data']['user_id']
    validate()

def validate():#登录验证
    global token
    url = "https://api.shisanshui.rtxux.xyz/auth/validate"
    headers = {'x-auth-token': token}
    res = requests.get(url, headers=headers)
    print('登录成功！')
    #print(res.json())

def logout():#注销
    global token
    url = "https://api.shisanshui.rtxux.xyz/auth/logout"
    headers = {'x-auth-token': token}
    res = requests.post(url, headers=headers)

def rank():#排行榜
    url = "https://api.shisanshui.rtxux.xyz/rank"
    res = requests.get(url)
    info = res.json()
    print('排行榜:')
    for i in info:
        print(i)

def get_history_list():#获取历史战局列表
    global token,user_id
    url = "https://api.shisanshui.rtxux.xyz/history?page=1&limit=10&player_id="
    url += str(user_id)
    headers = {'x-auth-token': token}
    res = requests.get(url, headers=headers)
    info = res.json()
    print('user_id为' , user_id , '的历史战局:')
    for i in info['data']:
        print(i)

def get_history_detail():#获取历史战局详情
    global token,id
    url = "https://api.shisanshui.rtxux.xyz/history/"
    url += str(id)
    headers = {'x-auth-token': token}
    res = requests.get(url, headers=headers)
    info = res.json()
    print("战局id为" + str(id) + '的战局详情为:')
    print(info)
if __name__ == '__main__':
    login()#登录及验证
    print('开启战局！')
    startgame()#开启战局
    postcards()#出牌
    print('出牌成功！')
    get_history_list()
    get_history_detail()
    #rank()