import requests
from bs4 import BeautifulSoup

def main():
    # 创建一个session，用于解决验证码和cookies等问题
    session = requests.session()
    # 用来储存结果的列表
    items =[]
    # 访问地址
    url = 'https://movie.douban.com/top250'

    # 模拟请求头文件
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36'

    }
    # 生成请求信息
    response = requests.get(url,headers = headers)

    # 生成BeautifulSoup对象，利用lxml解析
    soup =  BeautifulSoup(response.text,'lxml')

    # 利用选择器解析html
    divs = soup.select('.grid_view > li > div > .pic')

    # 获取提取并合并需要的信息
    for div in divs:
        src = div.select('a')[0]['href']
        name = div.select('a > img')[0]['alt']
        item = {
            'src':src,
            'name':name,
        }
        items.append(item)
    return items

if __name__ == '__main__':
    print(main())