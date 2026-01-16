# -*- coding: utf-8 -*-
# @Time    : 2018/12/1 9:36
# @Author  :  'zyx'
# @Email   : 458757014@qq.com
# @File    : test_commonPage.py
# @Software: PyCharm Community Edition

import pytest
from Common.my_log import logger
from PageObject.drafts_page import DraftsPage

class TestDraft:

    def test_del_draft_success(self):
        del_name = "成功删除邮件草稿箱的草稿测试"
        logger.info(del_name)
        #前置：跳转到草稿箱页面
        #步骤：1.选中要删除的邮件 2.点击删除button

