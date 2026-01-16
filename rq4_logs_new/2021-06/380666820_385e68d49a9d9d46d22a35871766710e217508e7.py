# -*- coding: utf-8 -*-

import requests
import json


HEADER = {
    'Host':'stuhealth.jnu.edu.cn',
    'Content-Type':'application/json',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'Accept-Encoding':'gzip',
    'Accept-Language':'zh-CN,zh;q=0.9',
    'Connection':'keep-alive',
    'Origin':'https://stuhealth.jnu.edu.cn',
    'Referer':'https://stuhealth.jnu.edu.cn/',
}

def jnu_daka_login(login_data):
    print ("模拟登录中...")

    login_data = json.dumps(login_data)
    
    response = requests.post(
        'https://stuhealth.jnu.edu.cn/api/user/login',
        data = login_data,
        headers = HEADER,
    )
    
    message = json.loads(response.content)
    print(message['meta']['msg'])

if __name__ == "__main__":
    # 从返回结果来看，有登录成功
    login_data = {
        'password': 'D9At_8avTYlYJmdDYR29Ew*=',
        "username": "1834451023",
    }
    jnu_daka_login(login_data)
   

