# coding:utf-8
import requests
import json
import time
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import re

my_sender = '1126711290@qq.com'
my_pass = 'ziehcuvcvfibhbaa'
my_user = '1126711290@qq.com'


def mail(str, link):
    try:
        mail_msg = '<a href="' + link + '">商品链接</a>'
        msg = MIMEText(mail_msg, 'html', 'utf-8')
        msg['From'] = formataddr(["大头", my_sender])
        msg['To'] = formataddr(["1126711290", my_user])

        msg['Subject'] = str

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        server.login(my_sender, my_pass)
        server.sendmail(my_sender, [my_user, ], msg.as_string())
        server.quit()
    except Exception:
        pass

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
}
proxies = {
    "https": "127.0.0.1:1080"
}
getMoto = "https://www.sephora.com/api/users/profiles/current/product/P436871"
motoUrl = "http://www.sephora.com/product/lip-maestro-mini-set-P436871"

# getUd = "https://www.sephora.com/api/users/profiles/current/product/P429051"
# udUrl = "https://www.sephora.com/product/backtalk-eye-face-palette-P429051"

# getYsl = "https://www.sephora.com/api/users/profiles/current/product/P419415"

# getga405 = "https://www.sephora.com/api/users/profiles/current/product/P393411"
# ga405Url = "https://www.sephora.com/product/lip-maestro-P393411?icid2=you%20may%20also%20like:p393411:product&skuId=1664267"
for i in range(1000):
#    motoHTML = requests.get(getMoto, headers=headers, proxies=proxies).content.decode('utf-8')
    # motoRet = json.loads(motoHTML)
    # motoStock = motoRet['currentSku']['actionFlags']['isAddToBasket']

    # udHTML = requests.get(getUd, headers=headers, proxies=proxies).content.decode('utf-8')
    # udRet = json.loads(udHTML)
    # udStock = udRet['currentSku']['actionFlags']['isAddToBasket']

    # yslHTML = requests.get(getYsl, headers=headers, proxies=proxies).content.decode('utf-8')

    motoHtml = requests.get(getMoto, headers=headers).content.decode('utf-8')

    # ga405HTML = requests.get(getga405, headers=headers, proxies=proxies).content.decode('utf-8')
    # ga405Ret = json.loads(ga405HTML)
    # ga405Stock = ga405Ret['regularChildSkus'][0]['actionFlags']['isAddToBasket']

    # if motoStock:
     #    print('!!!!!!!!!!!!!!')
      #   mail("摩托车", motoUrl)
     #else:
      #   print("摩托车无货")

    # if udStock:
    #     print('!!!!!!!!!!!!!!')
    #     mail("ud", udUrl)
    # else:
    #     print("UD无货")

    motoFlag = re.search(r'errorCode', motoHtml)
    if motoFlag:
        pass;
    else:
        mail("摩托", motoUrl)
        print("moto有货")
    # if ga405Stock:
    #     mail("405", ga405Url)
    # else:
    #     print("405无货")

    time.sleep(100)