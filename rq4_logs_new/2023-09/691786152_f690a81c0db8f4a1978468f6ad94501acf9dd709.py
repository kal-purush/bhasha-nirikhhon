# MAG-M

from email import header
import requests
import random
import string


Medhot = 404
url = "127.0.0.1"
phpmyadmin_port = 0
timeout_retry = 0
accuracy = {'HFish':0}

HFish_headers = {
        "Host":url,
        'Connection':'close'
}

def Variable_input(phpmyadmin_port):
    phpmyadmin_port = input("发现phpmyadmin端口？")
    if phpmyadmin_port == 0:
        phpmyadmin_port = 9301

def if_Medhot(timeout_retry):
    global Medhot
    try:
        if 200 == requests.get("http://"+url).status_code:
            Medhot = "http://"
        elif 200 == requests.get("https://"+url).status_code:
            Medhot = "https://"
    except Exception as error :
        print("Error1：",error)
        if timeout_retry <= 3:
            timeout_retry += 1
            if_Medhot(timeout_retry)
        else:
            exit()

def HFish(accuracy,phpmyadmin_port):
    #第一验证
    try:
        if 'window.alert("操作失败")' in (requests.post(Medhot+url+"/login", params=HFish_headers).text):
            accuracy['HFish'] += 20
        if 'window.alert("操作失败")' in (requests.post(Medhot+url+"/"+''.join(random.sample(string.ascii_letters + string.digits, 8)), params=HFish_headers).text):
            accuracy['HFish'] += 50
    except Exception as error :
        print("Error2：",error)

    #第二验证
    try:
        sss=requests.get(Medhot+url+":"+str(phpmyadmin_port)+"/index.php?db=&table=&token=0900719cf14fbbcc2c84071211fcae01&lang=hy").status_code
        if 200 != requests.get(Medhot+url+":"+str(phpmyadmin_port)+"/index.php?db=&table=&token=0900719cf14fbbcc2c84071211fcae01&lang=hy").status_code:
            accuracy['HFish'] +=30
    except Exception as error :
        print("Error3：",error)

    


def statistics(accuracy):#统计
    if accuracy['HFish'] > 0:
        print("HFish蜜罐："+str(accuracy['HFish'])+"%")
    print("结束")

def main():
    Variable_input(phpmyadmin_port)
    if_Medhot(timeout_retry)
    HFish(accuracy,phpmyadmin_port)
    statistics(accuracy)
main()