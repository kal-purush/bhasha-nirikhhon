#!/usr/bin/env python
# -*- coding: utf-8 ^*-

import sys
from os.path import dirname
from os.path import sep
from os import pardir
from os import path
from nose.tools import eq_
# import pymysql

from DbUtil.dbUtil import dbUtil
pardir_path = dirname(path.abspath(__file__)) + sep + pardir + sep
sys.path.append(pardir_path)
# from constants import constants
# from Manage_message_list import manage_message_list
import subprocess


class test_manage_message_list():

    conn = None

    # python_cmd = subprocess.check_output(["which", "python"]).strip()
    python_cmd = "/usr/local/bin/python"

    def setup(self):
        self.conn = dbUtil.connect()
        dbUtil.create_table(self.conn, 'test_table')

    def teardown(self):
        dbUtil.delete_table(self.conn, 'test_table')
        dbUtil.disConnect(self.conn)

    def test_insert(self):
        expected = 1
        print(self.python_cmd)
        cmd = str(self.python_cmd) + " " + pardir_path + "manage_message_list.py insert test_table test_message"
        print(cmd)
        actual = subprocess.check_output(cmd.strip().split(" "))

        print(actual)
        eq_(actual, expected)

    # @raises(IOError)
    # def test_connect_err(self):
        # constants.DB_INFO_INI = 'not_exist_file'
        # try:
        # dbUtil.connect()
        # finally:
        # constants.DB_INFO_INI = 'db_info.ini'