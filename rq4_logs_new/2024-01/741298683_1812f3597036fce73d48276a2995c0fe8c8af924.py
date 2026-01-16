import time
from urllib3 import encode_multipart_formdata
    def searchContent(self, key, quick, pg=1):
    def fetch(self, url, data=None, headers={}, cookies=""):
        if data is None:
            data = {}
        rsp = requests.get(url, params=data, headers=headers, cookies=cookies)
    def postBinary(self, url, data: dict, boundary=None, headers={}, cookies={}):
        if boundary is None:
            boundary = f'--dio-boundary-{int(time.time())}'
        headers['Content-Type'] = f'multipart/form-data; boundary={boundary}'
        # print(headers)
        fields = []
        for key, value in data.items():
            fields.append((key, (None, value, None)))
        m = encode_multipart_formdata(fields, boundary=boundary)
        data = m[0]
        rsp = requests.post(url, data=data, headers=headers, cookies=cookies)
        rsp.encoding = 'utf-8'
        return rsp

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File  : 樱花动漫.py
# Author: DaShenHan&道长-----先苦后甜，任凭晚风拂柳颜------
# Author's Blog: https://blog.csdn.net/qq_32394351
# Date  : 2024/1/7

import sys

sys.path.append('..')
try:
    from base.spider import Spider as BaseSpider
except ImportError:
    from t4.base.spider import BaseSpider
import json
import time
import base64
import re
import io
import tokenize
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Util.Padding import unpad

"""
配置示例:
t4的配置里ext节点会自动变成api对应query参数extend,但t4的ext字符串不支持路径格式，比如./开头或者.json结尾
api里会自动含有ext参数是base64编码后的选中的筛选条件
 {
    "key":"hipy_t4_樱花动漫",
    "name":"樱花动漫(hipy_t4)",
    "type":4,
    "api":"http://192.168.31.49:5707/api/v1/vod/樱花动漫",
    "searchable":1,
    "quickSearch":1,
    "filterable":1,
    "ext":"https://jihulab.com/qiaoji/open/-/raw/main/yinghua"
}
"""


class Spider(BaseSpider):  # 元类 默认的元类 type
    private_key: str = 'MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDA+5YTt3w1q/0WGw+TWyCSHTAeYiwBqAqDWot1u/1hoeANpED8gtW1AxB1mYNDQ+9eR8Ml+JC13+ME6RHjEbN4+n9V9OP90c81G0qSjBQ/DKQiMIFjbTH97RjVMtswf96tqwe4Rs/DT2ym6MP4P7QvJcxrFz5VVQXyOtUxhpMc9oktWuk0XKE8Mozu1FM879RknlM6WmJL85Wl/BnZrd+/AQbzziceELGrBfjbc1UOFAxYq2kA10H3o+Z4oOIODxUtXeh4R2oH3vHb4Ynnw6reXED5KsE3u1EO5HMQZyN16TZMTIps32bPe+vQlAT6V5nGcqXGT9fntjqIxJB0T9G3AgMBAAECggEBAKP6Yuh4BZP5g0CwV8jHKuLc6FE469mwdtZsLooo5cF68c3Fnu6xIXQAmZDDk3SpmhCLe7edASF5jwZSIL/H/68xcteQEdZP2/htKy1g16dHT4Q5oQfh9hOkznACGZuZW5ZH+HRNvyZfK5ybtkEPqERTouHwSyfo6feMpDDD/+cf3h1//7JKXKA7JPEU420YucsjQwjMuu5xdPa0TPqEc5mIbOBj753Pzn4GCScM+FRqJWr2x8e+KDPcPY8CUDLBSWxGLsB0A7+bEq/EiAQkbx09QKTwwxRLgVXjBbvyPB8BOuJpPM9BHx+vFcm5WSbkJdRI4qVFtEdsN/gDfFkwcjkCgYEA8Z8i/fTFRnzyvp9Pp8E+bSaYlvpTLUZ1KYNStaDg/BqlYGgGK1Jh90qjvRbBoiIjeBQd3IFLT4pFdd7Z9drLFdvqB22SNeVQU57kir/B6NY5G7yOjXB4qN17F4S3GubYIEcjF0W1tG/uOqqzb8FxrLJTK8WiFudbBt2ioCO4pJsCgYEAzHd8MctmD1Z1eM/xusvX1yCwGpxBuHT+ymThzLXyI6Ej0Q50jOQlf3cTyY/FgGbvAMz+oBybkEwE80gu7CPi0WPs+yCpAIB4+Th7afsrRylQI1ZWoRovaRmsyjnkIw0Mnj06VYNYPtkzm/OViRIqf4ESTTGas24bDm5DuwM9gxUCgYBwg4BR7gdnWYvYRGtdXNlrDowD0jGlZaftWt/LAE2EWAwmpooo5kYEV9eDl/M3QtptckCti++77FGIH+wzVl03op6KMvXg7xXGurkF+2GawRb62YUwS+2EBQ7q1rxFZLXD4hxvG+EPUwgGfbLtGZGLr8aXHYLrU3TJ769pDvlOfQKBgAFlAzzXtU9/eHele3GZuFQoTeswi6Y1bhN1UrDxwMALdlITtinL2JGg/0qNp3wzt4ea3lW7PDhkvFfocyF7MS3ab6Ba3aw6NBkHEJhtdSMcHgbPrPGWWyJtYWdTs8GlciOWKVKx/aUYGCkFJUz1CcMq3zQVlYeJxbd4ew/Iet/tAoGBAMRfvG1iLQAlS3AGaQeRwVxnvpciDn+7/sUCf8DEOk8Bqg4/ytJDTDrWufCtwmpsXmp6AUQig9mNKj7z26wSNbwYdzPsncK+sGRlS7eLAzzcv1a+1pghOOGDuQNzwlFOcauhkrcqjeKmu7OiKD48pvh3ZICiIWS1YL7LuMfUwHRJ'
    key: str = 'fQiG3YWTpQEYHNFTxJXCBaZrcCkkpfxH'
    iv: str = '1238389483762837'
    token: str = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJBcHBUbyIsImlhdCI6MTcwMDA3MTcwMiwiZXhwIjoxNzMxNjA3NzAyLCJuYmYiOjE3MDAwNzE3MDIsInN1YiI6IkFwcFRvIiwianRpIjoiYzRjNTAzOTQxYTM4NWI1MDMyMTAyYmY3Yzk1OGY4MzEiLCJkYXRhIjp7InVzZXJfaWQiOjI0ODc1NCwidXNlcl9jaGVjayI6ImUzYmQ3NmNhNTJhMGY4NjAwMTdjNjdkZGUwN2QzZTM3IiwidXNlcl9uYW1lIjoiaGV6aWh1aSJ9fQ.4LWs3rNL-os8_Pqa9LgKtvVG5f0aIxVyAjYIagvO1F4'
    ic: str = 'bmXes2xsCWvsSdfYav0s9D78Ly7w1o%2BOYXApKx6SUd4NWKsTsapbS52l7y%2FsTVCM2kcoLws2jryaDQlHLse5fxD2B2VXZXfaQo0eMTOv2Xq7CKoPa51uVt8WiIY2SPztc7wxGE89%2Fcw2Q3n85uUT3A%3D%3D'
    api: str = 'http://60.204.185.245:7090/appto/v1'
    api_cofig: str = api + '/config/get?p=android'
    api_home: str = api + '/home/cateData?id=1'
    api_cate: str = api + '/vod/getLists'
    api_search: str = api + '/vod/getVodSearch'
    api_detail: str = api + '/vod/getVod?__platform=android&__ic=' + ic
    api_parse: str = api + '/parsing/proxy'

    def getName(self):
        return "樱花动漫"

    def init_api_ext_file(self):
        """
        这个函数用于初始化py文件对应的json文件，用于存筛选规则。
        执行此函数会自动生成筛选文件
        @return:
        """
        ext_file = __file__.replace('.py', '.json')
        print(f'ext_file:{ext_file}')
        ext_file_dict = self.homeContent(True)['filters']
        with open(ext_file, mode='w+', encoding='utf-8') as f:
            f.write(json.dumps(ext_file_dict, ensure_ascii=False))

    def init(self, extend=""):
        """
        初始化加载extend，一般与py文件名同名的json文件作为扩展筛选
        @param extend:
        @return:
        """
        self.key = 'fQiG3YWTpQEYHNFTxJXCBaZrcCkkpfxH'
        self.iv = '1238389483762837'
        self.token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJBcHBUbyIsImlhdCI6MTcwMDA3MTcwMiwiZXhwIjoxNzMxNjA3NzAyLCJuYmYiOjE3MDAwNzE3MDIsInN1YiI6IkFwcFRvIiwianRpIjoiYzRjNTAzOTQxYTM4NWI1MDMyMTAyYmY3Yzk1OGY4MzEiLCJkYXRhIjp7InVzZXJfaWQiOjI0ODc1NCwidXNlcl9jaGVjayI6ImUzYmQ3NmNhNTJhMGY4NjAwMTdjNjdkZGUwN2QzZTM3IiwidXNlcl9uYW1lIjoiaGV6aWh1aSJ9fQ.4LWs3rNL-os8_Pqa9LgKtvVG5f0aIxVyAjYIagvO1F4'

        # cipher_text = 'qebgGqODxKrWujD2TaoZoyh6aeYntZLsTaZW8MQvVNORiHLLi/o/cEEEXkSs1vfHEuiURxMTr9zL2Kwbffz2pDvLCamrK1tuxIcczsaI7cfsAh5gcgyzOOdZUszoU45ZgS9Bi4HZ3RDHZAWEqP0JyTaH32oDxgO9nx/xhe1xyVRyj9y5gkfPVt2AP94I8/DwDI9CEstcPzGfRNHYxlCuGMtnMvVwzonmkNDyOw+N5xr2ZI3P8O1cj04mCEAE6/d2tGSU/o+EOyWVLWWw0R3hoduHvjAtuMbBt6WNCcsL51zWceiTcsl34acqd9Pya3lA52ZFRojME/R4zdTzMbvwrOEF3Up0eB58//+NM6k4tW+cBNAHDGgO8t7hcfSG/TnJLQ7Gigrn899dGbFeQMCEQf0zxMxfgYBDkADs0aza9N2QIXChgEDVFDvw0vE2m6b+bJfswmbrhNEIM//95XpMv68HEP3jCTwFEehattwHjyYE4fJoH3OtPO++pDiZltaxdizfFD6WmTO+v4qub1QfpUEXWHzG6nqDA9MioigiTRdRAb4PEMioBCJS5/gel+PGRja1cR1NtBjl4RKpSPstwrmczG8qKQrn2Zjwkz8f4OdnEBKRLaWzb/hVDAQgqRfUyohEdnQyocaYsnB5lJoMSp93QCzwAuHYueyMh2fNpcemWGfs58wrZr5VFSBaQDYv/HjsNntoPaCDpoQIV9DJKiqQsiYMblsz/Mdbnlfd1vI5X69TVPvItzh26zXjgpFBKsiFDOtvd3bNe92XnTH3jiDgBQbqoL6jUbvhNQapqRUZxovWk30UQ0YTQnqODkorKdrEB1IeNhKt0epurJG/G7A6DQ6/6lZ7pAwlYVlfXzFgZOGNb1Hs52j2F+sGvgWY00BAH1eHtpHPPdckL25JMKai3QDQUAVBxAI8njCmcNUPfl9kDZe62QMNJwaZKD9eaOjNrdVsKjah9gkiWL8BnDxJeGErZyrvKhyqg0fwNvyn8wEfeUlwRzfDysrvjdBz3'
        # self.decode_rsa(cipher_text)
        # extend内容进行解密后可以获取key,iv,token等参数
        # if extend.startswith('http'):
        #     try:
        #         r = self.fetch(extend)
        #         print(r.text)
        #     except Exception as e:
        #         print(f'加载ext扩展发生错误:{e}')

    def isVideoFormat(self, url):
        pass

    def manualVideoCheck(self):
        pass

    def homeContent(self, filterable=False):
        """
        获取首页分类及筛选数据
        @param filterable: 能否筛选，跟t3/t4配置里的filterable参数一致
        @return:
        """
        filter_names = {
            'area': '地区',
            'class': '分类',
            'director': '导演',
            'lang': '语言',
            'star': '明星',
            'state': '状态',
            'version': '版本',
            'year': '年份',
        }
        r = self.fetch(self.api_cofig)
        ret = r.json()
        data = self.decode(ret['data'])
        # print(data)
        result = {}
        classes = []
        filters = {}
        for tp in data.get('get_type') or []:
            classes.append({
                'type_name': tp['type_name'],
                'type_id': tp['type_id']
            })
            tp_filters = []
            for key, value in tp['type_extend'].items():
                tp_filters.append({
                    'key': key,
                    'name': filter_names.get(key) or key,
                    'value': [{'n': '全部', 'v': ''}] + [{'n': i, 'v': i} for i in value.split(',') if i]
                })
            filters[tp['type_id']] = tp_filters

        result['class'] = classes
        if filterable:
            result['filters'] = filters
        return result

    def homeVideoContent(self):
        """
        首页推荐列表
        @return:
        """
        r = self.fetch(self.api_home)
        ret = r.json()
        data = self.decode(ret['data'])
        # print(data)
        d = []
        for section in data['sections']:
            items = section['items']
            for item in items:
                d.append({
                    'vod_name': item['vod_name'],
                    'vod_id': item['vod_id'],
                    'vod_pic': item['vod_pic'],
                    'vod_remarks': item['vod_remarks'],
                })
        result = {
            'list': d
        }
        return result

    def categoryContent(self, tid, pg, filterable, extend):
        """
        返回一级列表页数据
        @param tid: 分类id
        @param pg: 当前页数
        @param filterable: 能否筛选
        @param extend: 当前筛选数据
        @return:
        """
        page_count = 21  # 默认赋值一页列表21条数据
        fls = extend.keys()  # 哪些刷新数据
        # ?type_id=1&area=&lang=&year=&order=time&type_name=&page=1&pageSize=21
        params = {'page': pg, 'pageSize': page_count, 'tid': tid, 'type_name': ''}
        for fl in fls:
            params[fl] = extend[fl]
        r = self.fetch(self.api_cate, data=params)
        print(r.url)
        ret = r.json()
        data = self.decode(ret['data'])
        d = data['data']
        result = {
            'list': d,
            'page': pg,
            'pagecount': 9999 if len(d) >= page_count else pg,
            'limit': 90,
            'total': data['total'],
        }
        return result

    def detailContent(self, ids):
        """
        返回二级详情页数据
        @param ids: 一级传过来的vod_id列表
        @return:
        """
        # id=110102
        vod_id = ids[0]
        params = {'id': vod_id}
        r = self.fetch(self.api_detail, data=params)
        print(r.url)
        ret = r.json()
        data = self.decode(ret['data'])
        # print(data)

        vod = {"vod_id": vod_id,
               "vod_name": data['vod_name'],
               "vod_pic": data['vod_pic'],
               "type_name": data['vod_en'],
               "vod_year": data['vod_year'],
               "vod_area": data['vod_area'],
               "vod_remarks": data['vod_remarks'],
               "vod_actor": data['vod_actor'],
               "vod_director": data['vod_director'],
               "vod_content": data['vod_blurb'],
               "vod_play_from": data['vod_play_from'],
               }
        vod_play_list = data['vod_play_list']
        vod_play_urls = []
        for vod_play in vod_play_list:
            vod_play_url = '#'.join([url['name'] + '$' + url['url'] for url in vod_play['urls']])
            vod_play_urls.append(vod_play_url)
        vod['vod_play_url'] = '$$$'.join(vod_play_urls)
        result = {
            'list': [vod]
        }
        return result

    def searchContent(self, wd, quick=False, pg=1):
        """
        返回搜索列表
        @param wd: 搜索关键词
        @param quick: 是否来自快速搜索。t3/t4配置里启用了快速搜索，在快速搜索在执行才会是True
        @param pg: 页数
        @return:
        """
        # ?wd=%E4%B8%89%E5%A4%A7%E9%98%9F&page=1&type=
        params = {'wd': wd, 'type': '', 'page': pg}
        r = self.fetch(self.api_search, data=params)
        print(r.url)
        ret = r.json()
        data = self.decode(ret['data'])
        # print(data)
        d = data['data']
        result = {
            'list': d
        }
        return result

    def playerContent(self, flag, id, vipFlags):
        """
        解析播放,返回json。壳子视情况播放直链或进行嗅探
        @param flag: vod_play_from 播放来源线路
        @param id: vod_play_url 播放的链接
        @param vipFlags: vip标识
        @return:
        """
        headers = {
            'Content-Type': 'multipart/form-data; boundary=--dio-boundary-1205762094',
            'token': self.token,
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1',
        }
        params = {'play_url': id, 'label': '主线', 'key': 'mp4'}
        r = self.postBinary(self.api_parse, data=params, boundary='--dio-boundary-1205762094', headers=headers)
        # print(r.request.body.decode())
        ret = r.json()
        data = self.decode(ret['data'])
        # print(data)
        url = data['url']
        parse = 0
        result = {
            'parse': parse,  # 1=嗅探,0=播放
            'playUrl': '',  # 解析链接
            'url': url,  # 直链或待嗅探地址
            # 'header': headers,  # 播放UA
        }
        return result

    config = {
        "player": {},
        "filter": {}
    }
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.54 Safari/537.36",
        "Host": "www.baidu.com",
        "Referer": "https://www.baidu.com/"
    }

    def localProxy(self, param):
        return [200, "video/MP2T", action, ""]

    # -----------------------------------------------自定义函数-----------------------------------------------
    def decode(self, text):
        return json.loads(self.aes_cbs_decode(text, self.key, self.iv))

    def decode_rsa(self, text):
        return json.loads(self.rsa_private_decode(text, self.private_key))

    def eval_computer(self, text):
        """
        自定义的字符串安全计算器
        @param text:字符串的加减乘除
        @return:计算后得到的值
        """
        localdict = {}
        self.safe_eval(f'ret={text.replace("=", "")}', localdict)
        ret = localdict.get('ret') or None
        return ret

    def safe_eval(self, code: str = '', localdict: dict = None):
        code = code.strip()
        if not code:
            return {}
        if localdict is None:
            localdict = {}
        builtins = __builtins__
        if not isinstance(builtins, dict):
            builtins = builtins.__dict__.copy()
        else:
            builtins = builtins.copy()
        for key in ['__import__', 'eval', 'exec', 'globals', 'dir', 'copyright', 'open', 'quit']:
            del builtins[key]  # 删除不安全的关键字
        # print(builtins)
        global_dict = {'__builtins__': builtins,
                       'json': json, 'print': print,
                       're': re, 'time': time, 'base64': base64
                       }  # 禁用内置函数,不允许导入包
        try:
            self.check_unsafe_attributes(code)
            exec(code, global_dict, localdict)
            return localdict
        except Exception as e:
            return {'error': f'执行报错:{e}'}

    # ==================== 静态函数 ======================
    @staticmethod
    def check_unsafe_attributes(string):
        """
        安全检测需要exec执行的python代码
        :param string:
        :return:
        """
        g = tokenize.tokenize(io.BytesIO(string.encode('utf-8')).readline)
        pre_op = ''
        for toktype, tokval, _, _, _ in g:
            if toktype == tokenize.NAME and pre_op == '.' and tokval.startswith('_'):
                attr = tokval
                msg = "access to attribute '{0}' is unsafe.".format(attr)
                raise AttributeError(msg)
            elif toktype == tokenize.OP:
                pre_op = tokval

    @staticmethod
    def aes_cbs_decode(ciphertext, key, iv):
        # 将密文转换成byte数组
        ciphertext = base64.b64decode(ciphertext)
        # 构建AES解密器
        decrypter = AES.new(key.encode(), AES.MODE_CBC, iv.encode())
        # 解密
        plaintext = decrypter.decrypt(ciphertext)
        # 去除填充
        plaintext = unpad(plaintext, AES.block_size)
        # 输出明文
        # print(plaintext.decode('utf-8'))
        return plaintext.decode('utf-8')

    @staticmethod
    def rsa_private_decode(ciphertext, private_key):
        # 计算需要添加的等号数
        b64_ciphertext = ciphertext
        num_padding = 4 - (len(b64_ciphertext) % 4)
        if num_padding < 4:
            b64_ciphertext += "=" * num_padding
        print(len(ciphertext))
        print(len(b64_ciphertext))
        print(b64_ciphertext)
        # 将密文转换成byte数组
        ciphertext = base64.b64decode(b64_ciphertext)

        # ciphertext = ciphertext.encode()
        # print('{}'.format(ciphertext))
        # 构建RSA解密器
        decrypter = PKCS1_OAEP.new(private_key)
        # 解密
        plaintext = decrypter.decrypt(ciphertext)
        # 输出明文
        print(plaintext.decode('utf-8'))
        return plaintext.decode('utf-8')


if __name__ == '__main__':
    # 在线aes测试 https://config.net.cn/tools/AES.html
    # 分类页:http://60.204.185.245:7090/appto/v1/home/cateData?id=1
    # 推荐页:http://60.204.185.245:7090/appto/v1/config/get?p=android
    spider = Spider()
    spider.init('https://jihulab.com/qiaoji/open/-/raw/main/yinghua')
    # spider.init_api_ext_file()  # 生成筛选对应的json文件

    # print(spider.homeContent(True))
    # print(spider.homeVideoContent())
    # print(spider.categoryContent('1', 1, True, {'year': '2024'}))
    # print(spider.detailContent([110078]))
    # print(spider.searchContent('斗罗大陆'))
    print(spider.playerContent(None, 'f1d7d074f624e993e425f|11d1d091b0b28|31613145e4a7c|518737c8650978', None))

    # spider.searchContent('斗罗大陆')
    # key = "fQiG3YWTpQEYHNFTxJXCBaZrcCkkpfxH"
    # iv = "1238389483762837"
    # ciphertext = '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'
    # print(len(key), len(iv), len(ciphertext))
    # data = spider.aes_cbs_decode(ciphertext, key, iv)
    # data = json.loads(data)
    # print(data)
    # ciphertext = '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'
    # data = spider.aes_cbs_decode(ciphertext, key, iv)
    # print(data)