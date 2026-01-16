# !/usr/bin/python
# -*- coding: utf-8 -*-

import DbUtil
import Config
import Logger

class CreateTable(object):
    """docstring for Preprocessor"""
    def __init__(self):
        super(CreateTable, self).__init__()
        self.__db = DbUtil.DbUtil(Config.INPUT_DB_HOST, Config.INPUT_DB_USERNAME, Config.INPUT_DB_PASSWORD, Config.INPUT_DB_DATABASE, Config.INPUT_DB_CHARSET)
        self.__logger = Logger.Logger(__name__)


    def create_table(self):
        sql='''
            CREATE TABLE person_body_status_face_track(
	        camera_id char(20) not null,
		frame_id char(20),
		body_id char(20),
		body_stat char(10),
		body_track char(20),
		face_id char(20),
		face_track char(20),
		face_pose char(10),
		face_pose_stat char(10),
		face_pose_stat_time char(10),
		face_emotion char(10),
		yawn char(10),
		unix_timestamp char(20),
		pose_stat_time char(20)
	    )engine=innodb default charset=utf8
	'''
	self.__db.create(sql)


        sql='''
            CREATE TABLE person_body_status_pose_stat_midlle(
	        camera_id char(20) not null,
		face_id char(20),
		pose_stat_time char(20),
		face_pose_stat_time char(20),
		body_stat char(10),
		face_pose char(10),
		face_emotion char(10)
	    )engine=innodb default charset=utf8
	'''
	self.__db.create(sql)

        sql='''
            CREATE TABLE person_body_status_face_pose_stat(
	        camera_id char(20) not null,
		face_id char(20),
		pose_stat_time char(20),
		body_stat char(10),
		face_pose char(10),
		face_emotion char(10),
		face_pose_stat char(10)
	    )engine=innodb default charset=utf8
	'''
	self.__db.create(sql)

        sql='''
            CREATE TABLE person_body_status_course_info(
	        camera_id char(20) not null,
		face_id char(20),
		pose_stat_time char(20),
		body_stat char(10),
		face_pose char(10),
		face_emotion char(10),
		face_pose_stat char(10),
		course_name char(20)
	    )engine=innodb default charset=utf8
	'''
	self.__db.create(sql)

        sql='''
            CREATE TABLE school_course_info(
	        course_name char(20),
		start_time char(20),
		end_time char(20),
		ds char(10)
	    )engine=innodb default charset=utf8
	'''
	self.__db.create(sql)