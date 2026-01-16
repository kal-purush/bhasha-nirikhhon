import time

import requests
from bs4 import BeautifulSoup


def get_content(url):
    try:
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0'
        response = requests.get(url, headers={'User-Agent': user_agent})
        response.raise_for_status()
        response.encoding = response.apparent_encoding
    except Exception as e:
        print("爬取错误")
    else:

        print(response.url)
        print("爬取成功!")
        return response.content


def save_img(img_src):
    if img_src is None:
        return
    try:
        print(img_src)
        urlArr = img_src.split('?')
        if len(urlArr) == 2:
            url = urlArr[1]
        else:
            url = urlArr[0]
        headers = {"User-Agent": 'Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0'}

        requests.packages.urllib3.disable_warnings()
        res = requests.get(url=url, headers=headers, verify=False)
        data = res.content
        filePath = "E:\\pic\\" + str(int(time.time())) + ".jpg"
        with open(filePath, "wb+") as f:
            f.write(data)
    except Exception as e:
        print(e)


def downloadImg(sigleArticle):
    if sigleArticle is None:
        return

    soup = BeautifulSoup(sigleArticle, 'html.parser')

    imgList = soup.find_all('p', class_="GsImageLabel")
    for img in imgList:
        atag = img.find('a')
        if atag:
            save_img(atag['href'])


def getAllArticle(content):

    soup = BeautifulSoup(content, 'html.parser')
    divObj = soup.find_all('div', class_="tit")
    for item in divObj:
        link = item.find('a')
        if link:
            articleUrl = link['href']
            sigleArticle = get_content(articleUrl)
            downloadImg(sigleArticle)
            arr = articleUrl.split(".shtml")
            for i in range(2, 10):
                url = arr[0] + "_" + str(i) + ".shtml"
                sigleArticle = get_content(url)
                downloadImg(sigleArticle)


if __name__ == '__main__':
    for i in range(2, 5):
        print(i)
        url = "https://www.gamersky.com/ent/xz/"
        articleUrl = "https://www.gamersky.com/ent/202107/1406688.shtml"
        content = get_content(url)
        getAllArticle(content)

