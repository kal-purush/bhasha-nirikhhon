# -*- coding: utf-8 -*-
import unittest
import HtmlTestRunner
import xmlrunner
from public import Mod
#导入所有的testCase
from testCase import *

testunit = unittest.TestSuite()

#将测试用例添加到testSuite
testunit.addTest(unittest.makeSuite(comOrder.Test))
#html测试报告
# runner = HtmlTestRunner.HTMLTestRunner(output="allTests")
#xml测试报告
runner = xmlrunner.XMLTestRunner(output="xmlReports")
runner.run(testunit)