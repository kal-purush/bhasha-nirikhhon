# -*- coding: utf-8 -*-
import time
import datetime
import json

import arrow
import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth

from ini_conf import MyIni


class FetchData(object):

    def __init__(self):
        self.myini = MyIni()
        self.hbc_conf = self.myini.get_hbc()
        self.kakou_ini = {'host': '10.47.187.165', 'port': 80}
        self.hbc_ini = {'host': '127.0.0.1', 'port': 5000}
        
        self.id_flag = self.hbc_conf['id_flag']
        self.step = self.hbc_conf['step']
        self.kkdd = self.hbc_conf['kkdd']
        self.city = self.hbc_conf['city']

        self.kakou_status = False
        self.hbc_status = False

    def __del__(self):
        del self.myini

    def kakou_post(self, carinfo):
        headers = {'content-type': 'application/json'}
        url = 'http://{0[host]}:{0[port]}/hbc'.format(self.hbc_ini)
        data = {'carinfo': carinfo}
        try:
            r = requests.post(url, headers=headers, data=json.dumps(data))
            if r.status_code == 200 or r.status_code == 429:
                return r
            else:
                self.hbc_status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.hbc_status = False
            raise

    def get_cltxs(self, id_flag, step=100):
        #last_id = self.id_flag + self.step
        url = 'http://{0[host]}:{0[port]}/rest_hz_kakou/index.php/{1}/kakou/cltxs/{2}/{3}'.format(
            self.kakou_ini, self.city, id_flag, id_flag+step)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.kakou_status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.kakou_status = False
            raise

    def get_cltxmaxid(self):
        url = 'http://{0[host]}:{0[port]}/rest_hz_kakou/index.php/{1}/kakou/cltxmaxid'.format(
            self.kakou_ini, self.city)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.kakou_status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.kakou_status = False
            raise

    def fetch_data(self):
        """获取卡口车辆信息"""
        maxid = self.get_cltxmaxid()['maxid']
        if maxid <= self.id_flag:
            # 没有新的数据 返回1
            return
        info = self.get_cltxs()
        if info['total_count'] == 0:
            if self.id_flag + self.step < maxid:
                self.id_flag += self.step
            else:
                self.id_flag = maxid
            self.myini.set_hbc(self.id_flag)
            return
        # 过滤无效车牌
        def data_valid(i)
            if i['kkdd_id'] and i['hphm'] != '' and i['hphm'] != '-':
                return i
        r = self.kakou_post(filter(data_valid, info['items']))
        if r.status_code == 201:
            self.myini.set_hbc(info['items'][-1]['id'])
        elif r.status_code == 429:
            time.sleep(2)

    def main_loop(self):
        while 1:
            if self.kakou_status and self.hbc_status:
                try:
                    self.fetch_data()
                    time.sleep(1)
                except Exception as e:
                    time.sleep(1)
            else:
                try:
                    if not self.kakou_status:
                        self.get_cltxmaxid()
                        self.kakou_status = True
                    if not self.hbc_status:
                        self.get_gdhbc_by_hphm(u'粤L12345', '02')
                        self.hbc_status = True
                except Exception as e:
                    time.sleep(1)

if __name__ == '__main__':  # pragma nocover
    fd = FetchData()
    #print fd.get_cltxmaxid()
    id_flag = 307388026
    while 1:
        c = fd.get_cltxs(id_flag, 1000)
        r = fd.hbc_post(c['items'])
        id_flag += 1000
        if id_flag >= 308388026:
            break
        time.sleep(1)
        print id_flag
        #print r.headers
        print r.status_code
        #print r.text