from bs4 import BeautifulSoup
import requests
from collections import Counter
import re
import threading
import time
wc = Counter()#建字典
wc["C"] = 0
wc["C++"] = 0
wc["C#"] = 0
wc["PYTHON"] = 0
wc["JAVA"] = 0
wc["JAVASCRIPT"] = 0
wc["PHP"] = 0
wc["HTML"] = 0
wc["SQL"] = 0
wc["CSS"] = 0
wc["R"] = 0
wc["BASH"] = 0
wc["RUBY"] = 0
wc["PERL"] = 0
wc["SCALA"] = 0
wc["SWIFT"] = 0
wc["GO"] = 0
wc["DELPHI"] = 0
wc["TYPESCRIPT"] = 0



def getLink(page):
    befh = "https://www.1111.com.tw/job-bank/job-index.asp?si=4&sk=100400,100600,100300&fs=0&page="
    Host = befh+str(page)#換頁
    res = requests.get(Host)
    soup = BeautifulSoup(res.text,"lxml")
    choose = soup.select('div.jbInfoin')#為了選標題頁連結
    link = ["https:"+choose1.select_one('a')['href']for choose1 in choose]
    global links #將內頁連結裝在一起
    links += link
    print("success")



class getLinkThread (threading.Thread):#跑主頁的thread
    def __init__(self,page):
        threading.Thread.__init__(self)
        self.page=page
    def run(self):
        getLink(self.page)




def getWord(link):
    res = requests.get(link)
    print(link)
    soup = BeautifulSoup(res.text,"lxml")
    text = []#建空白list為了放內文upper後的值
    text1 = soup.select("dl.dataList")#求才條件
    for word in text1:
        k = word.text.upper()
        text.append(k)
    a = re.findall('[A-Z]+[+#]*',"%s"%text)#取出我們要的值但會有重複
    text2 = []#建空白list為了放過濾好的值
    for language in a:#過濾重複的值
        if language not in text2:
            text2.append(language)
    from collections import Counter
    for target in text2:#計次
        if target in wc:
            wc[target]+=1
    return wc


class getWordThread (threading.Thread):#跑內頁的thread
    def __init__(self,link):
        threading.Thread.__init__(self)
        self.link=link
    def run(self):
        getWord(self.link)



links = []
threads=[]
for page in range(1,151):
    Thread=getLinkThread(page)
    threads.append(Thread)
for i in threads:
    i.start()
for i in threads:
    i.join()
threadsword = []
for link in links:
    Thread=getWordThread(link)
    threadsword.append(Thread)
for i in threadsword:
    i.start()
    time.sleep(1)
for i in threadsword:
    i.join()


#將結果製成長條圖
from collections import OrderedDict
import numpy as np
import matplotlib.pyplot as plt
language = OrderedDict(wc.most_common())
xticks = np.arange(len(language))
plt.bar(xticks, language.values(), align='center')
plt.xticks(xticks, list(language.keys()),rotation=75)
plt.title("The most popular programming language")
plt.show()


import json
with open ('1111_crawler.json','w') as f:#建json檔
    json.dump(language, f)
print(wc.most_common())





