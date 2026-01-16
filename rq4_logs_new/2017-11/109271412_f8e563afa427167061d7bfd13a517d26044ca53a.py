import requests
import time
import re
from PIL import Image


captcha_url = 'https://www.zhihu.com/captcha.gif?r={}&type=login&lang=cn'
main_url = 'https://zhihu.com'
login_url = 'https://zhihu.com/login/email'

# a dict contains the position of downside character position, will be replaced with the true position
captcha_pos = {"img_size":[200,44],"input_points":[[15.49,27.64],[152.29,22.05]]}
data = {'email':'username', # username to login
        'password':'password', # password for the login
        'captcha_type':'cn', # the type of captcha type, 7 chinese character,with 1 or 2 upside down
        '_xsrf':'', # the _xsrf extract from the page
        'captcha':captcha_pos,
        }


def clean_headers(data,delimiter=':',sep='\n'):
    """ clean the headers or cookies copied from firefox"""
    tmp = {}
    lines = [line.strip() for line in data.split(sep) if len(line) > 5 and 'Content-Length' not in line]
    for line in lines:
        k,v = line.split(delimiter,maxsplit=1)
        tmp[k] = v.strip()
    return tmp


def get_r():
    """generate the timestamp and format the captcha_url"""
    tmp = str(time.time()*1000)[:13]
    return captcha_url.format(tmp)

if __name__ == "__main__":
    headers_content = """Host: www.zhihu.com
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0
Accept: */*
Accept-Language: zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3
Accept-Encoding: gzip, deflate, br
X-Xsrftoken: d59a6b5cbb326bc05328a62d9cbad4e6
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
X-Requested-With: XMLHttpRequest
Referer: https://www.zhihu.com/
"""

    cookies_content = """q_c1=0ac7d18b42ef49fca2ed2d2329f08806|1508755382000|1503108834000; \
    q_c1=0ac7d18b42ef49fca2ed2d2329f08806|1508755382000|1503108834000; \
    _zap=085b7d4a-cc5d-4d46-9484-eddfae6335f9; \
    r_cap_id="NGNkYzMzMzVkNjMxNDliMGFhNjI4NjVmN2I4NjZiOTA=|1509270328|3d95cad72848b29ea4ebf4fc23d19625ce3a5daf"; \
    cap_id="ZDIxODgxNjg4YjJkNDliZTg4MmJhMTA1NDI5YTRhNTU=|1509270328|6329931af284734c55371d889da754e086deb76f"; \
    __utma=51854390.1832205118.1503829450.1507911395.1509268878.5; \
    __utmz=51854390.1507911395.4.4.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/question/31851633; \
    __utmv=51854390.000--|2=registration_date=20140104=1^3=entry_date=20170819=1; d_c0="ADCCAsLhSAyPTj8w_8ikHEn95wo0plMro9M=|1503829449"; \
    aliyungf_tc=AQAAAGtsiFglFgoA2wBa2uB6FZZPn0iP; _xsrf=d59a6b5cbb326bc05328a62d9cbad4e6; __utmb=51854390.0.10.1509268878; \
    __utmc=51854390; _xsrf=d59a6b5cbb326bc05328a62d9cbad4e6; \
    l_cap_id="ZGYxMmNhYjI1Mjk4NDZlYmJhZjg5MTc0ZmNlMGQyYTM=|1509270328|54b243a5996b47a2732eeebf946f2392f578d50d"
    """

    headers = clean_headers(headers_content)
    cookies = clean_headers(cookies_content,delimiter='=',sep=';')
    sess = requests.session()
    r = sess.get(url=main_url)
    if r.ok:
        tmp =re.search('input type="hidden" name="_xsrf" value="([a-z0-9]*)',r.text)
        if tmp:
            print(tmp.group(1))
            data['_xsrf'] = tmp.group(1)
            print('the xsrf is: %s' % tmp.group(1))

    r = sess.get(get_r())
    if r.ok:
        with open('xx.gif','wb') as file:
            file.write(r.content)
        print('save the captcha,please open and input the position')
        img = Image.open('xx.gif')
        img.show()

    pos1 = input('input the position 1 ')
    pos2 = input('input the position 2')
    data['captcha']['input_points'][0][0] = int(pos1) * 45 + 0.25
    data['captcha']['input_points'][1][0] = int(pos2) * 48 + 0.67
    headers['X-Xsrftoken'] = data['_xsrf']

    cookies['_xsrf'] = data['_xsrf']
    r = sess.post(url=login_url,data=data,headers=headers,cookies=cookies)
    try:
        print(r.json())
    except:
        pass
    print(sess.cookies.items())

    # print(req.text)


