import requests
from ThirteenWater import *
import json

token = ""
id = ""
user_id = ""
# cards = ""
def startgame():#开启战局
    global token,id
    url = "https://api.shisanshui.rtxux.xyz/game/open"
    headers = {'x-auth-token': token}
    res = requests.post(url, headers=headers)
    info = res.json()
    if info['status'] == 0:
        print('开启战局')
        id = info['data']['id']
        cards = info['data']['card']
        print('当前手牌:',cards)
    else:
        print('游戏无法进行!')
    return info

def postcards(cards):#出牌
    global token,id
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
    if info['status'] == 0:
        print("出牌成功!")
        return data['card']
    else:
        print('出牌失败，请检查你的连接是否正常！')

def register(username,password):#注册
    url = "https://api.shisanshui.rtxux.xyz/auth/register"
    headers = {'content-type': "application/json"}
    data = {
        "username":username,
        "password":password
    }
    res = requests.post(url,data=json.dumps(data),headers=headers)

def bind(student_number,student_password):#绑定
    url = "https://api.shisanshui.rtxux.xyz/game/bind"
    headers = {'x-auth-token':token,'content-type': "application/json"}
    data = {
        "student_number": student_number,
        "student_password": student_password
    }
    res = requests.post(url,headers = headers,data=json.dumps(data))
    info = res.json()
    if info['status'] == 0:
        print("绑定成功!")

def register_with_bind(username,password,student_number,student_password):#注册+绑定
    url = "https://api.shisanshui.rtxux.xyz/auth/register2"
    headers = {'content-type': "application/json"}
    data = {
        "username":username,
        "password":password,
        "student_number": student_number,
        "student_password": student_password
    }
    res = requests.post(url,data=json.dumps(data),headers=headers)
    info = res.json()
    if info['status'] == 0:
        print("注册并绑定成功!")

def login(username,password):#登录
    global token,user_id
    url = "https://api.shisanshui.rtxux.xyz/auth/login"
    headers = {'content-type': "application/json"}
    data = {
        "username": username,
        "password": password
    }
    res = requests.post(url, data=json.dumps(data), headers=headers)
    info = res.json()
    if info['status'] == 0:
        token = info['data']['token']
        user_id = info['data']['user_id']
        return validate()
    else:
        print('登录失败，请检查你的密码是否输入正确！')

def validate():#登录验证
    global token
    url = "https://api.shisanshui.rtxux.xyz/auth/validate"
    headers = {'x-auth-token': token}
    res = requests.get(url, headers=headers)
    info = res.json()
    if info['status'] == 0:
        print('登录成功！')
    else:
        print('登录失败，请检查你的密码是否输入正确！')
    return info['status']
    #print(res.json())

def logout():#注销
    global token
    url = "https://api.shisanshui.rtxux.xyz/auth/logout"
    headers = {'x-auth-token': token}
    res = requests.post(url, headers=headers)
    info = res.json()
    return json
def get_rank():#排行榜
    url = "https://api.shisanshui.rtxux.xyz/rank"
    res = requests.get(url)
    info = res.json()
    print('排行榜:')
    for i in info:
        print(i)
    return info

def get_history_list(playerid,page):#获取历史战局列表
    global token
    url = "https://api.shisanshui.rtxux.xyz/history?page="+ page +"&limit=50&player_id=" + playerid
    headers = {'x-auth-token': token}
    res = requests.get(url, headers=headers)
    info = res.json()
    print('user_id为' , playerid , '的历史战局:')
    for i in info['data']:
        print(i)
    return info

def get_history_detail(game_id):#获取历史战局详情
    global token
    url = "https://api.shisanshui.rtxux.xyz/history/"
    url += game_id
    print(url)
    headers = {'x-auth-token': token}
    res = requests.get(url, headers=headers)
    info = res.json()
    print("战局id为" + str(id) + '的战局详情为:')
    print(info)
    return info
