#!/app/billapp/scripts/tools/python2.7/bin/python
# coding: UTF-8

import logging
import logging.handlers
import cx_Oracle
import os
import json
import sys
import time
import datetime

#from lxml import etree
from collections import OrderedDict

class kpi_process:
    ''' kpi文件解析
    '''

    @classmethod
    def parseone(cls, json_kpi):
        '''解析单个kpi内容

        对于content按照kpi_id排序，保证不会因为kpi内容顺序不一致，
        导致入库时映射错误
        '''
        stat = []
        kpi_arr = []
        if len(json_kpi) > 0:
            stat.append(json_kpi['header']['serv_id'])
	    t1 = time.strptime(json_kpi['header']['timestamp'],"%Y-%m-%d %H:%M:%S")
	    y,m,d,h,mm,ss = t1[0:6]
            stat.append(datetime.datetime(y,m,d,h,mm,ss))

            for kpi_item in json_kpi['content']:
                kpi_arr.append(kpi_item)
            kpi_arr.sort(key=lambda k: (k.get('kpi_id', 0)))
            #print(kpi_arr)

            for kpi_item in kpi_arr:
                type = kpi_item['stat_type']
                value = kpi_item['value']
                # 计时型：starttime,endtime,count,average,time_total,time_max,time_min,data_total,fail_total,dist0,dist1,dist2,dist3
                # 计数型：count,starttime,endtime
                if len(value) < 0:
                    continue

                if type == "TIME":  # 待确认
                    # 插入顺序根据sql定
                    arr = value.split(',')
                    stat.append(arr[0])
                    stat.append(arr[1])
                    #stat.append(arr[2])
                    stat.append(arr[4])
                    stat.append(arr[3])
                elif type == "COUNT":   
                    stat.append(arr[0])

        return stat, len(stat)

    @classmethod
    def parsefile(cls, filename, biz_ids, biz_queue):
        '''解析单个文件的kpi内容, 只处理biz_id符合要求的
        '''
        stat_arr = []
        
        if not os.path.isfile(filename):
            return stat_arr, -1
        
        # 根据后缀和其他条件过滤下，防止有一些临时文件跟非kpi stat文件
        suffix = os.path.splitext(filename)[1]
        if suffix == '.kpi' and 'kpi_stat' in filename and 'PS' in filename:
            file=open(filename, 'r')
            try:
                all_lines = file.readlines()
            finally:
                file.close()
            
            # json.load ?
            for kpi in all_lines:
                json_kpi = json.loads(kpi, object_pairs_hook=OrderedDict)
                biz_id = json_kpi['header']['biz_id']
                
                # 处理biz_id符合要求的
                if int(biz_id) in biz_ids:
                    stat, _ = cls.parseone(json_kpi)
                    # print(size)
                    # 每个业务对应一个queue
                    biz_queue[int(biz_id)].put(stat)

        return biz_queue, len(biz_queue)


class conn_oracle:
    ''' 提供oracle处理功能
    '''

    def __init__(self):
        self.db_conn = None

    def connect(self, user, pwd, dsn, enconding='utf-8'):
        ''' 连接数据库 
        '''
        self.db_conn = cx_Oracle.connect(user=user, password=pwd, dsn=dsn, encoding=enconding)

    def close(self):
        '''关闭连接
        '''
        self.db_conn.close()

    def query(self, sql, params=None):
        ''' 执行查询语句
            sql: select语句
            params: sql语句的参数
            return: 查询结果集和记录数
        '''
        rows=None
        try:
            cursor=self.db_conn.cursor()
            # 调用prepare()方法，发送sql，并对sql进行预编译
            cursor.prepare(sql)
            # 调用execute()，不要传递sql语句，否则预编译sql无效，执行的还是execute()方法的sql
            if params==None:
                cursor.execute(None)
            else:
                cursor.execute(None, params)

            rows = cursor.fetchall()
            rowcount = cursor.rowcount
        # 不要处理异常
        # except Exception as e:
        #     print(e)
        finally:
            cursor.close()
        return rows, rowcount

    def update(self, sql, params=None):
        '''执行insert、update、delete语句
        
        sql：insert、update、delete语句
        params：sql语句的参数
        return: 影响的记录数
        '''
        cursor=self.db_conn.cursor()
        try:
            # 调用prepare()方法，发送sql，并对sql进行预编译
            cursor.prepare(sql)
            if params:
                cursor.execute(None,params)
            else:
                cursor.execute(None)
            self._ou_rowcount=cursor.rowcount
        finally:
            cursor.close()
        return self._ou_rowcount

    def commit(self):
        '''
        事务提交
        '''
        self.db_conn.commit()

    def rollback(self):
        '''
        事务回滚
        '''
        self.db_conn.rollback()


class logger:
    ''' 提供日志功能
    '''

    # 先读取XML文件中的配置数据
    # 由于config.xml放置在与当前文件相同的目录下，因此通过 __file__ 来获取XML文件的目录，然后再拼接成绝对路径
    # 这里利用了lxml库来解析XML
    #root = etree.parse(os.path.join(os.path.dirname(__file__), 'config.xml')).getroot()
   
    # 读取日志文件保存路径
    # logpath = root.find('logpath').text 
    logpath = "./"   

    # 读取日志文件容量，转换为字节
    # logsize = 1024*1024*int(root.find('logsize').text)
    logsize = 1024*1024*8
   
    # 读取日志文件保存个数
    # lognum = int(root.find('lognum').text)
    lognum = 3    

    # FATAL = 50 ERROR = 40 WARNING = 30 INFO = 20 DEBUG = 10
    # loglevel = int(root.find('loglevel').text) 
    loglevel = 10

    # 日志文件名：由用例脚本的名称，结合日志保存路径，得到日志文件的绝对路径
    logname = os.path.join(logpath, sys.argv[0].split('/')[-1].split('.')[0])

    # 初始化logger
    log = logging.getLogger()

    # 日志格式，可以根据需要设置
    fmt = logging.Formatter('%(asctime)s %(filename)s %(lineno)d [%(levelname)s] [%(thread)d] %(message)s', '%Y-%m-%d %H:%M:%S')

    # 日志输出到文件，这里用到了上面获取的日志名称，大小，保存个数
    handle1 = logging.handlers.RotatingFileHandler(logname+'.log', maxBytes=logsize, backupCount=lognum)
    handle1.setFormatter(fmt)

    # 同时输出到屏幕，便于实施观察
    handle2 = logging.StreamHandler(stream=sys.stdout)
    handle2.setFormatter(fmt)

    log.addHandler(handle1)
    log.addHandler(handle2)

    # 设置日志基本，这里设置为INFO，表示只有INFO级别及以上的会打印
    log.setLevel(loglevel)

    # 日志接口，用户只需调用这里的接口即可，这里只定位了INFO, WARNING, ERROR, DEBUG四个级别的日志，可根据需要定义更多接口
    # classmethod 修饰符对应的函数不需要实例化，不需要 self 参数，但第一个参数需要是表示自身类的 cls 参数，可以来调用类的属性，类的方法，实例化对象等。
    @classmethod
    def info(cls, msg):
        cls.log.info(msg)
        return

    @classmethod
    def warning(cls, msg):
        cls.log.warning(msg)
        return

    @classmethod
    def error(cls, msg):
        cls.log.error(msg)
        return

    @classmethod 
    def debug(cls, msg):
        cls.log.debug(msg)
        return