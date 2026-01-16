# -*- coding: utf-8 -*-
import os
import time
import json
#import shutil
import logging
#import cStringIO

import requests
#from PIL import Image

import helper
import helper_wm
import img_builder
from ini_conf import MyIni
from my_logger import debug_logging

debug_logging(u'logs/error.log')

logger = logging.getLogger('root')


class HbcCompare(object):

    def __init__(self):
        self.myini = MyIni()
        self.hbc_conf = self.myini.get_hbc()
        self.kakou_ini = {'host': '10.47.187.165', 'port': 80}
        self.cgs_ini = {'host': '10.47.222.45', 'port': 8080,
                        'username': 'test1', 'password': 'test12345'}
        self.hbc_ini = {'host': '127.0.0.1', 'port': 5000,
                        'username': 'test1', 'password': 'test12345'}
        #self.cgs2_ini = {'host': '10.47.222.45', 'port': 8081}
        self.id_flag = self.hbc_conf['id_flag']
        self.step = self.hbc_conf['step']
        self.kkdd = self.hbc_conf['kkdd']
        self.city = self.hbc_conf['city']
        self.city_name = self.hbc_conf['city_name']

        self.kakou_status = False
        self.cgs_status = False
        self.hbc_status = False
        # 黄标车集合 dict
        self.gdhbc_dict = {}
        # 号牌颜色字典 dict
        self.hpys_dict = {
            'WT': u'白底黑字',
            'YL': u'黄底黑字',
            'BU': u'蓝底白字',
            'BK': u'黑底白字',
            'QT': u'其他'
        }
        # 机号字典 dict
        self.jh_dict = {}
        # 设备代号 dict
        self.sbdh_dict = {}
        # 方向编号 dict
        self.fxbh_dict = {
            'NS': u'北向南',
            'SN': u'南向北',
            'EW': u'东向西',
            'WE': u'西向东',
            'IN': u'东向西',
            'OT': u'西向东'
        }
        self.hbc_img_path = self.hbc_conf['hbc_img_path']#u'd://videoandimage'
        self.wz_img_path = self.hbc_conf['wz_img_path']
        self.hbc_img_dict = {}
        #self.hbc_img = helper.hbc_img()


    def get_gdhbc_by_hphm(self, hphm, hpzl):
        headers = {
            'content-type': 'application/json'
        }
        url = u'http://{0[host]}:{0[port]}/hbc/{hphm}/{hpzl}'.format(
            self.cgs_ini, hphm=hphm, hpzl=hpzl)
        try:
            r = requests.get(url, headers)
            if r.status_code == 200:
                return json.loads(r.text)
            elif r.status_code == 404:
                return None
            else:
                self.cgs_status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.cgs_status = False
            raise

    def get_gdhbc_all(self):
        headers = {
            'content-type': 'application/json'
        }
        url = 'http://{0[host]}:{0[port]}/hbc_all'.format(self.cgs_ini)
        try:
            r = requests.get(url, headers)
            if r.status_code == 200:
                items = json.loads(r.text)['items']
                print 'hbc_num:%s' % len(items)
                for i in items:
                    self.gdhbc_dict[(i['hphm'], i['hpzl'])] = i['ccdjrq']
            else:
                self.cgs_status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.cgs_status = False
            raise

    def get_cltxs(self):
        last_id = self.id_flag + self.step
        url = 'http://%s:%s/rest_hz_kakou/index.php/%s/kakou/cltxs/%s/%s' % (
            self.kakou_ini['host'], self.kakou_ini['port'], self.city,
            self.id_flag, last_id)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.kakou_status = False
                raise Exception('url: %s, status: %s, %s' % (
                    url, r.status_code, r.text))
        except Exception as e:
            self.kakou_status = False
            raise

    def get_cltxmaxid(self):
        url = 'http://%s:%s/rest_hz_kakou/index.php/%s/kakou/cltxmaxid' % (
            self.kakou_ini['host'], self.kakou_ini['port'], self.city)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.kakou_status = False
                raise Exception('url: %s, status: %s, %s' % (
                    url, r.status_code, r.text))
        except Exception as e:
            self.kakou_status = False
            raise

    def get_kkdd(self):
        """获取卡口地点代码"""
        url = 'http://%s:%s/kkdd/%s' % (
            self.hbc_ini['host'], self.hbc_ini['port'], self.kkdd)
        headers = {
            'content-type': 'application/json'
        }
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                items = json.loads(r.text)['items']
                for i in items:
                    self.jh_dict[i['kkdd_id']] = i['cf_id']
                    self.sbdh_dict[i['kkdd_id']] = i['sbdh']
            else:
                self.hbc_status = False
                raise Exception('url: %s, status: %s, %s'
                                % (url, r.status_code, r.text))
        except Exception as e:
            self.hbc_status = False
            raise

    def get_hbc_img(self):
        """获取违章黄标车路标图片"""
        url = 'http://%s:%s/wzimg/%s' % (
            self.hbc_ini['host'], self.hbc_ini['port'], self.kkdd)
        headers = {
            'content-type': 'application/json'
        }
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                base_path = u'%s\%s' % (self.wz_img_path, self.kkdd)
                helper.makedirs(base_path)
                for i in json.loads(r.text)['items']:
                    filename = u'%s\%s_%s.jpg' % (base_path, i['kkdd_id'], i['fxbh_code'])
                    # 如果图片不存在则抓图
                    if not os.path.exists(filename):
                        helper.get_url_img(i['img_url'], filename)
                    self.hbc_img_dict[(i['kkdd_id'], i['fxbh_code'])] = filename
            else:
                self.hbc_status = False
                raise Exception('url: %s, status: %s, %s'
                                % (url, r.status_code, r.text))
        except Exception as e:
            self.hbc_status = False
            raise

    def check_hbc_img_by_hphm(self, date, hphm):
        headers = {
            'content-type': 'application/json'
        }
        url = 'http://%s:%s/hbc/img/%s/%s/%s' % (
            self.hbc_ini['host'], self.hbc_ini['port'], date, hphm, self.kkdd)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.hbc_status = False
                raise Exception('url: %s, status: %s, %s'
                                % (url, r.status_code, r.text))
        except Exception as e:
            self.hbc_status = False
            raise

    def check_hbc(self, hphm, hpzl):
        """检测是否黄标车"""
        ccdjrq = self.gdhbc_dict.get((hphm, hpzl), None)
        if not ccdjrq:
            return False
        h = self.get_gdhbc_by_hphm(hphm, hpzl)
        if not h:
            return False

        if arrow.get(ccdjrq).date() != arrow.get(h['ccdjrq']).date():
            return False
        return True

    def add_hbc(self, data):
        """添加黄标车信息"""
        url = 'http://%s:%s/hbc' % (
            self.hbc_ini['host'], self.hbc_ini['port'])
        headers = {
            'content-type': 'application/json'
        }
        try:
            r = requests.post(url, headers=headers, data=json.dumps(data))
            if r.status_code == 201:
                return json.loads(r.text)
            else:
                self.hbc_status = False
                raise Exception('url: %s, status: %s, %s' % (
                    url, r.status_code, r.text))
        except Exception as e:
            self.hbc_status = False
            raise

    def cmpare_hbc(self, i):
        """根据车辆信息比对黄标车"""
        # 判断车牌是否需要黄标车查询
        f_hphm = helper.fix_hphm(i['hphm'], i['hpys_code'])
        if f_hphm['hpzl'] == '00':
            return
        # 是否在黄标车集合里面
        if not self.check_hbc(f_hphm['hphm'], f_hphm['hpzl']):
            return
        jgsj = arrow.get(i['jgsj'])
        # print u'黄表车: %s, 号牌颜色: %s' % (i['hphm'], i['hpys'])
        hbc_img = self.check_hbc_img_by_hphm(
            jgsj.format('YYYY-MM-DD'), i['hphm'])
        imgpath = ''
        if hbc_img['total_count'] == 0:
            try:
                path = u'%s/%s/违章图片目录' % (
                    self.hbc_img_path, jgsj.format(u'YYYY年MM月DD日'))
                # 图片名称
                name = u'机号%s车道A%s%sR454DOK3T%sC%sP%s驶向%s违章黄标车违反禁令标志' % (
                    self.jh_dict[i['kkdd_id']], i['cdbh'],
                    jgsj.format(u'YYYY年MM月DD日HH时mm分ss秒'),
                    f_hphm['cpzl'], self.hpys_dict[i['hpys_code']],
                    i['hphm'], self.fxbh_dict.get(i['fxbh_code'], u'其他'))
                # 水印内容
                text = u'违法时间:%s 违法地点:%s%s\n违法代码:13441 违法行为:违章黄标车 设备编号:%s\n防伪码:%s' % (
                    i['jgsj'], self.city_name, i['kkdd'],
                    self.sbdh_dict[i['kkdd_id']], helper.get_sign())
                # 违章路标图片
                wz_img = self.hbc_img_dict.get((i['kkdd_id'], i['fxbh_code']), None)
##                if wz_img is not None:
##                    wz_img = u'hbc_img/'+wz_img
                imgpath = img_builder.get_img_by_url(
                    i['imgurl'], path, name, text, wz_img)
            except Exception as e:
                logger.error('url: %s' % i['imgurl'])
                logger.error(e)
                imgpath = ''

        data = {
            'jgsj': i['jgsj'],
            'hphm': i['hphm'],
            'kkdd_id': i['kkdd_id'],
            'hpys_code': i['hpys_code'],
            'fxbh_code': i['fxbh_code'],
            'cdbh': i['cdbh'],
            'imgurl': i['imgurl'],
            'imgpath': imgpath
        }
        # 添加黄标车信息到数据库
        self.add_hbc(data)

    def fetch_data(self):
        """获取卡口车辆信息"""
        maxid = self.get_cltxmaxid()['maxid']
        if maxid <= self.id_flag:
            # 没有新的数据 返回1
            time.sleep(1)
            return 1
        carinfo = self.get_cltxs()
        if carinfo['total_count'] == 0:
            if self.id_flag + self.step < maxid:
                self.id_flag += self.step
            else:
                self.id_flag = maxid
            self.myini.set_hbc(self.id_flag)
            time.sleep(1)
            # print 'id_flag: %s' % self.id_flag
            return 0
        elif carinfo['total_count'] < self.step:
            time.sleep(1)

        # 遍历列表并比对是否黄标车
        for i in carinfo['items']:
            if i['kkdd_id'] is not None:
                self.cmpare_hbc(i)

        self.id_flag = carinfo['items'][-1]['id']
        self.myini.set_hbc(self.id_flag)
        print '{city}_id_flag: {flag}'.format(city=self.city, flag=self.id_flag)
        return -1

    def main_loop(self):
        # 时间戳标记
        time_flag = time.time()
        # 加载初始化数据
        init_flag = False
        while 1:
            if not init_flag:
                try:
                    # 获取黄标车数据
                    self.get_gdhbc_all()
                    self.cgs_status = True
                    # 获取卡口地点数据
                    self.get_kkdd()
                    self.hbc_status = True

                    init_flag = True
                    print 'Init Finish'
                except Exception as e:
                    logger.error(e)
                    time.sleep(1)
            elif self.kakou_status and self.cgs_status and self.hbc_status:
                try:
                    # 当前时间大于时间戳标记时间2小时则更新黄标车数据
                    if time.time() - time_flag > 7200:
                        self.get_gdhbc_all()
                        time_flag = time.time()
                    self.fetch_data()
                except Exception as e:
                    logger.error(e)
                    time.sleep(1)
            else:
                try:
                    if not self.kakou_status:
                        self.get_cltxmaxid()
                        self.kakou_status = True
                    if not self.cgs_status:
                        self.get_gdhbc_by_hphm(u'粤L12345', '02')
                        self.cgs_status = True
                    if not self.hbc_status:
                        self.check_hbc_img_by_hphm('2015-09-26', u'粤L12345')
                        self.hbc_status = True
                except Exception as e:
                    #print (e)
                    time.sleep(1)
        del hbc


if __name__ == "__main__":
    hbc = HbcCompare()
    hbc.get_hbc_img()
    del hbc
##    hbc = HbcCompare()
##    #hbc.main_loop()
##    url = 'http://localhost/kakou/images/test2.jpg'
##    path = 'imgs'
##    name = 'test2'
##    text = u'Linsir.vi5i0n@hotmail.com'
##    hbc.get_img_by_url(url, path, name, text)
##    del hbc