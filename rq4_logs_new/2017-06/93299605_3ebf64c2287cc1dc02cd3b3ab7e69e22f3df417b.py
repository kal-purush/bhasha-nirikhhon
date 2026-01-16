# -*- coding:utf8 -*-

import urllib2
import os
import chardet
from bs4 import BeautifulSoup
import base64
import datetime

# 爱漫画网爬虫
# 作者： antchow
# 说明：本代码纯属写来用于提升个人技术以及技术交流，如有他人利用代码来获取商业利益，均与本人无关！

web_url = 'http://www.iimanhua.com/'
cartoon_py = raw_input("请输入漫画拼音:")
basic_url = web_url + u'imanhua/' + cartoon_py

# 控制请求次数，超过3次就放弃请求
retry_count = 0
while retry_count < 3:
    try:
        # 设置请求头
        req = urllib2.Request(basic_url)
        # req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
        # req.add_header('Accept-Encoding', 'gzip, deflate')
        # req.add_header('Accept-Language', 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3')
        # req.add_header('Connection', 'keep-alive')
        # req.add_header('Cookie', 'bdshare_firstime=' + str(time.time()) + '; '
        #                          'UM_distinctid=15c691a5ea2102-07c370fdf3ecee8-43564131-1fa400-15c691a5ea417b; '
        #                          'CNZZDATA1255100919=1694723486-1496407660-null%7C1496473325; alm_cpv_r_6621_fidx=1; '
        #                          'JXM733178=1; Pmy=; '
        #                          'JXD733178=1; '
        #                          'qtmhhis=2017-5-3-14-45-35%5E%5E%u6B7B%u795E%5E%5E%u7B2C655%u8BDD%5E%5E2%5E%5E261856%5E%5E9202_ShG_')
        # req.add_header('Host', 'www.iimanhua.com')
        # req.add_header('Referer', 'http://www.iimanhua.com/')
        # req.add_header('Upgrade-Insecure-Requests', '1')
        # req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0')

        html_1 = urllib2.urlopen(req).read()
        retry_count = 3
    except Exception, e:
        print u'第' + str(retry_count+1) + u'次请求失败!' + repr(e)
        if retry_count < 3:
            retry_count += 1
            print u'正在进行第 ' + str(retry_count+1) + u' 次请求...'
        else:
            print u'请求失败超过三次，将不再请求！'

if retry_count < 3:
    exit()

# 解决乱码问题
my_char = chardet.detect(html_1)
charset = my_char['encoding']
if charset == 'utf-8' or charset == 'UTF-8':
    html = html_1
else:
    html = html_1.decode('gb2312', 'ignore').encode('utf-8')

# 获取漫画名称
soup = BeautifulSoup(html)
cartoon_name = soup.find('div', class_='title').find('h1').get_text()
print u'正在下载漫画： ' + cartoon_name

# 创建文件夹
path = os.getcwd()
new_path = os.path.join(path, cartoon_name)
if not os.path.isdir(new_path):
    os.mkdir(new_path)

# 解析所有章节的URL
chapterURLList = []
chapterLI_all = soup.find('div', id='play_0').find_all('a')
for chapterLI in chapterLI_all:
    chapterURLList.append(chapterLI.get('href'))
    # print chapterLI.get('href')

# 遍历章节的URL
for chapterURL in chapterURLList:
    retry_count = 0
    while retry_count < 3:
        try:
            req = urllib2.Request(web_url + str(chapterURL))
            # req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            # req.add_header('Accept-Encoding', 'gzip, deflate')
            # req.add_header('Accept-Language', 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3')
            # req.add_header('Connection', 'keep-alive')
            # req.add_header('Cookie', 'bdshare_firstime=' + str(time.time()) + '; '
            #                          'UM_distinctid=15c691a5ea2102-07c370fdf3ecee8-43564131-1fa400-15c691a5ea417b; '
            #                          'CNZZDATA1255100919=1694723486-1496407660-null%7C1496473325; '
            #                          'alm_cpv_r_6621_fidx=1; JXM733178=1; Pmy=; JXD733178=1; '
            #                          'qtmhhis=2017-5-3-14-45-35%5E%5E%u6B7B%u795E%5E%5E%u7B2C655%u8BDD%5E%5E2%5E%5E261856%5E%5E9202_ShG_; '
            #                          'ASPSESSIONIDSCTRABCS=NGEOKFHAOELOMDOEDJMBHLND ')
            # req.add_header('Host', 'www.iimanhua.com')
            # req.add_header('Referer', 'http://www.iimanhua.com/imanhua/sishen/')
            # req.add_header('Upgrade-Insecure-Requests', '1')
            # req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0')
            html = urllib2.urlopen(req, timeout=120).read()
            retry_count = 3

        except Exception, e:
            print u'对 ' + web_url + str(chapterURL) + u'的第' + str(retry_count + 1) + u'次请求失败!' + repr(e)
            if retry_count < 3:
                retry_count += 1
                print u'正在进行第 ' + str(retry_count + 1) + u' 次请求...'
            else:
                print u'请求失败超过三次，将不再请求！'

    if retry_count < 3:
        break

    html = html.decode('gb2312', 'ignore').encode('utf-8')
    chapter_soup = BeautifulSoup(html)
    chapter_name = chapter_soup.find('h1').get_text()

    print u'正在下载章节： ' + chapter_name
    begin_time = datetime.datetime.now()

    # 获取当前章节所有加密的图片url, 并进行第一重解密
    index1 = html.index("qTcms_S_m_murl_e=\"")
    index2 = html.index("var qTcms_S_m_murl_e2")
    page_num_base64 = html[index1:index2]
    page_num_base64.strip()
    page_num_base64 = page_num_base64[18:len(page_num_base64) - 4]
    page_num = base64.decodestring(page_num_base64).split("$qingtiandy$")
    print u'本章共: ' + str(len(page_num)) + u' 张'

    for i in range(len(page_num)):
        print u'正在下载第： ' + str(i+1) + u'张'
        try:
            # 按照网站的js对加密的url进行第二重解密
            page_num[i] = page_num[i].replace("http://cartoon.jide123.cc/", "http://cartoon.shhh88.com/")
            page_num[i] = page_num[i].replace("http://cartoon.jide123.cc:8080/", "http://cartoon.shhh88.com/")
            page_num[i] = page_num[i].replace("http://cartoon.shhh88.com/", "http://cartoon.akshk.com/")
            if page_num[i].find(base64.decodestring("ZG16ai5jb20=")) != -1:
                page_num[i] = web_url + "qTcms_Inc/qTcms.Pic.FangDao.asp?p=" + base64.encodestring(page_num[i])
            if page_num[i].find("http:") == -1:
                page_num[i] = web_url + page_num[i]

            retry_count = 0
            while retry_count < 3:
                # 请求图片url获取图片
                try:
                    req = urllib2.Request(page_num[i])
                    # req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
                    # req.add_header('Accept-Encoding', 'gzip, deflate')
                    # req.add_header('Accept-Language', 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3')
                    # req.add_header('Connection', 'keep-alive')
                    # req.add_header('Cookie', '__cfduid=d04b794c06b2343358dc3e0e839d97ff81496485711')
                    # req.add_header('Host', 'www.iimanhua.com')
                    # #req.add_header('Referer', webURL + str(chapterURL))
                    # req.add_header('If-Modified-Since', 'Wed, 17 May 2017 11:02:30 GMT')
                    # req.add_header('If-None-Match', '"3d6bc413fdced21:0"')
                    # req.add_header('Upgrade-Insecure-Requests', '1')
                    # req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0')
                    content = urllib2.urlopen(req, timeout=120).read()
                    retry_count = 3
                except Exception, e:
                    if type(e) is urllib2.URLError:
                        print u'图片未找到，直接进入下一章！'
                        break
                    else:
                        print u'对 ' + page_num[i] + u'的第' + str(retry_count + 1) + u'次请求失败!' + repr(e)
                        if retry_count < 3:
                            retry_count += 1
                            print u'正在进行第 ' + str(retry_count + 1) + u' 次请求...'
                        else:
                            print u'请求失败超过三次，将不再请求！'

            if retry_count < 3:
                break

            # 保存图片
            img_name = chapter_name + str(i + 1)
            with open(cartoon_name + '/' + img_name + '.JPG', 'wb') as new_img:
                new_img.write(content)

        except Exception, e:
            print repr(e)
            break

    print chapter_name + u'下载完毕！共' + str(len(page_num)) + u'张，用时： ' + str((datetime.datetime.now() - begin_time).seconds) + u' s'

print "~~~~~~~~~~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
raw_input("Press <Enter> To Quit!")