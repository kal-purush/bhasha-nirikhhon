import urllib
import http.client as httplib
import json, time


filename = "Users.json"
urlAddr = "statistics.pandadastudio.com"
conn = httplib.HTTPConnection(urlAddr)

#Save data to json
def store(data):
    global filename
    with open(filename, 'w') as user:
        json.dump(data, user)
        user.close

#Retrieve
def read():
    global filename
    with open(filename, 'r') as user:
        data = json.load(user)
        return data
    return 0

#UID Simple Checker
def uidCheck(data):
    global conn
    conn.request("GET", "/player/simpleInfo?" + data)
    response = conn.getresponse()
    data = json.load(response)
    print("user load status: ",data['msg'])
    if data['code'] == 0:
        print(data['data']['name'], "<-   NAME¦¦¦RANK   ->" ,data['data']['title'])
        return data['data']
    else:
        print("用户错误或者不存在")
        return 0
    
#Gift Code Checker
def giftCode(user, code):
    global conn
    paramCODE = urllib.parse.urlencode({'uid': user, 'code':code})
    conn.request("GET", "/player/giftCode?" + paramCODE)
    response = conn.getresponse()
    data = json.load(response)
    print(user, "gift code status: ",data['msg'])

def UserOperation():
    #Print all information
    print("Function directed to user operation!")
    data = read()
    print("Data read complete!")
    for i in dict.keys(data):
        print(">>UID:", i, ">>名字:", data[i]['name'], ">>忍阶:",  data[i]['title'])
    print("是否增加？不增加请按0，删除角色请加-")
    while True:
        uid = input()
        if uid == '0':
            print("Operation complete!")
            break
        if uid in dict.keys(data):
            print("UID already available!")
            continue
        if '-' in uid:
            delete = uid.replace('-','')
            print(delete)
            if delete in dict.keys(data):
                del data[delete]
                print("Delete Complete!")
            else:
                print("User not in the key list")
            continue
        node = uidCheck(urllib.parse.urlencode({'uid': uid}))
        if node != 0:
            print("User found complete!", node['name'])
            data[uid] = node
            print("Success!")
    store(data)
        

def GiftCodeOperation():
    print("Please input the giftcode:")
    gift = input()
    data = read()
    for i in dict.keys(data):
        giftCode(i, gift)
    

#switcher
def switch():
    print("请输入你要执行的操作：\n1. User operation用户管理\n2. Gift code operation礼包码领取")
    keyword = input()
    switcher = {
        '1' : UserOperation,
        '2' : GiftCodeOperation
        }
    func = switcher.get(keyword, lambda : "不存在的指令")
    func()

switch();