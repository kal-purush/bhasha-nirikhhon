# !usr/bin/python
# -*- coding: utf-8 -*-

import DbUtil
import Config
import Logger
import random

class TestData(object):
    """docstring for TestData"""
    def __init__(self):
        super(TestData, self).__init__()
        self.__db = DbUtil.DbUtil(Config.INPUT_DB_HOST, Config.INPUT_DB_USERNAME, Config.INPUT_DB_PASSWORD, Config.INPUT_DB_DATABASE, Config.INPUT_DB_CHARSET)
        self.__logger = Logger.Logger(__name__)
        self.__courses = ['rest', 'math', 'chinese', 'english', 'physic', 'history', 'geography']
        self.__classes = 101
        self.__persons = 101


    def produce(self):
        ''''''
        sql = '''
            INSERT INTO  {0} (camera_id, face_id, pose_stat_time, body_stat, face_pose, face_emotion, face_pose_stat, course_name, class_id) VALUES 
        '''.format(Config.INTERMEDIATE_TABLE_TRAIN)

        for k in xrange(1,1000):
            vSqls = ''
            first = True
            for x in xrange(1, self.__classes):
                for y in xrange(1,self.__persons):
                    if not first:
                        vSqls += ','
                    vSqls += '''
                        ('{0}', '{1}', '1549682797', '{2}', '{3}', '{4}', '0', '{5}', '{0}')
                    '''.format(x, (x - 1) * 100 + y, random.randint(0, 5), random.randint(0, 2), random.randint(0, 2), self.__courses[random.randint(0, len(self.__courses) - 1)])
                    first = False
            self.__db.insert(sql + vSqls)
            self.__logger.info("Try to insert 10000 records to the table {0} in the database {1}".format(Config.INTERMEDIATE_TABLE_TRAIN, Config.INPUT_DB_DATABASE))

if __name__ == '__main__':
    doer = TestData()
    doer.produce()