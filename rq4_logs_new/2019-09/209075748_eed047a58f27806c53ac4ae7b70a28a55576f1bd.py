import requests
import urllib3
import json
import urllib
import urllib.request

headers = {
    'Accept': '*/*',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    'Referer':"https://item.jd.com/100000177760.html#comment"
}

# flag
flag = True

# https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv102&productId=100006154988&score=3&sortType=5&page=0&pageSize=10&isShadowSku=0&fold=1


def getjsontxt(url):
    try:
        r = requests.get(url, timeout = 30, headers = headers)
        r.raise_for_status()

        return r.text
    except:
        return "存在异常"


str = "https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv102&productId=100006154988&score=3&sortType=5&page=0&pageSize=100&isShadowSku=0&fold=0"
res = getjsontxt(str)
res = res.split('(')[1].split(')')[0]
print(res)

def json_op(result):
    global flag
    j = json.loads(result)
    comments = j["comments"]
    # print(comments)
    for i in comments:
        text = i["content"]
        if flag:
            flag = False
            with open("D://CCCC.txt",'w', encoding='UTF-8') as file:
                file.write(text + "\n-----------------------\n")
            file.close()
        else:
            with open("D://CCCC.txt", 'a', encoding='UTF-8') as file:
                file.write(text + "\n-----------------------\n")
            file.close()

json_op(res)