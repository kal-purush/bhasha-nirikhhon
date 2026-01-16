import http.cookiejar
import urllib.request
import re
import bz2


if __name__ == "__main__":

    #step1:获取cookie
    url = "http://www.pythonchallenge.com/pc/def/linkedlist.php"
    cookie = http.cookiejar.CookieJar() #声明一个CookieJar实例来保存cookie
    handler = urllib.request.HTTPCookieProcessor(cookie) #利用urllib库的HTTPCookieProcessor对象创建cookie处理器

    opener = urllib.request.build_opener(handler) #通过handler创建opener
    res = opener.open(url)
    print(res.read())
    for item in cookie:
        print(item.name,item.value) #info you+should+have+followed+busynothing...

    #step2:像第四关一样，不断获取下一页面 并将cookie保存
    html = "http://www.pythonchallenge.com/pc/def/linkedlist.php?busynothing={0}"
    content = "the next busynothing is 12345" #与第四关一样，从12345开始
    match_str = re.search(r"the next busynothing is (\d+)", str(content))
    cookie2 = []

    while match_str != None:
        next_num = match_str.group(1)
        print("next_num: ", next_num)
        html_now = html.format(next_num)
        content = opener.open(html_now).read()
        match_str = re.search(r"the next busynothing is (\d+)", str(content))

        for item in cookie:
            cookie2.append(item)


    cookie_str = ""
    for item in cookie2:
        cookie_str += item.value
        print(item.name,item.value)

    print(cookie_str)
    #BZh91AY%26SY%94%3A%E2I%00%00%21%19%80P%81%11%00%AFg%9E%A0+%00hE%3DM%B5%23%D0%D4%D1%E2%8D%06%A9%FA%26S%D4%D3%21%A1%EAi7h%9B%9A%2B%BF%60%22%C5WX%E1%ADL%80%E8V%3C%C6%A8%DBH%2632%18%A8x%01%08%21%8DS%0B%C8%AF%96KO%CA2%B0%F1%BD%1Du%A0%86%05%92s%B0%92%C4Bc%F1w%24S%85%09%09C%AE%24%90


    # #step3: 用bz2解密字符串
    cookie_rep =urllib.parse.unquote_to_bytes(cookie_str.replace("+"," ")) #将字符串中的"+"替换为空格
    cookie_decom = bz2.decompress(cookie_rep) #'is it the 26th already? call his father and inform him that "the flowers are on their way". he\'ll understand.'

    #step4: 由cookie_decom的结果知，跟第16关有关:mozart的父亲 Leopold 跟第13关有关：
    #mozart的父亲Leopold
    import xmlrpc.client
    php_html = "http://www.pythonchallenge.com//pc//phonebook.php"
    re_ans1 = xmlrpc.client.ServerProxy(php_html)
    print(re_ans1.phone("Leopold")) #555-VIOLIN

    #在地址栏输入：http://www.pythonchallenge.com/pc/return/violin.html
    #得到： no! i mean yes! but ../stuff/violin.php.
    #根据提示在地址栏输入：http://www.pythonchallenge.com/pc/stuff/violin.php  得到一个图片
    #step5:继续用cookie
    list(cookie)[0].value = "the+flowers+are+on+their+way"
    content = opener.open("http://www.pythonchallenge.com/pc/stuff/violin.php").read()
    print(content)##结果中包含：oh well, don\'t you dare to forget the balloons.
    #得到18关结果：http://www.pythonchallenge.com/pc/return/balloons.html



