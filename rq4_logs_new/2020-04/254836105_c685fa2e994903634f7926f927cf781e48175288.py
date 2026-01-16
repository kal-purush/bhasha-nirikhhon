import requests
from bs4 import BeautifulSoup as bs
import time


def puzzle():
    headers = {
        'authority': 'lichess.org',
        'accept': '*/*',
        'sec-fetch-dest': 'empty',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://lichess.org',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'referer': 'https://lichess.org/training/',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'lila2=6a6506b5f3f1d215351193db0ba140e37eaaebb0-sid=QMb1d1eF3WrJKuvmSzmc04&sessionId=9dzmeiUggQL9SQ4iMAlTIs'}

    r = c.get('https://lichess.org/training', headers=headers)
    soup = bs(r.content,"lxml")
    res =  soup.findAll("meta",{'property':'og:title'})[0]["content"]
    a = res.find("#")+1
    res = res[a:]
    b = res.find(" ")
    res = res[:b]
    return res

while True:
    with requests.Session() as c:
        data = {'username': 'akulchhillar','password': 'batman$0'}
        c.post("https://lichess.org/login?referrer=%2F",data=data)
        p_id = puzzle()
        url = "https://lichess.org/training/%s" %(p_id)
        headers = {
        'authority': 'lichess.org',
        'accept': '*/*',
        'sec-fetch-dest': 'empty',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://lichess.org',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'referer': 'https://lichess.org/training/%s' %(p_id),
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'lila2=6a6506b5f3f1d215351193db0ba140e37eaaebb0-sid=QMb1d1eF3WrJKuvmSzmc04&sessionId=9dzmeiUggQL9SQ4iMAlTIs'}
        data = {'win': 1}
        response = requests.post('https://lichess.org/training/%s/round2' %(p_id), headers=headers, data=data)
        print response.status_code
        print p_id
        print response.text
        time.sleep(10)
