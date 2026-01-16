# _*_ coding:utf-8 _*_
import os, sys
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.append(BASE_DIR)

# 配置文件
CONFIG_DIR = os.path.join(BASE_DIR, "Config", "config.ini")
SETTING_DIR = os.path.join(BASE_DIR, "Config", "setting.py")

# 测试用例目录
LOGINCASE_DIR = os.path.join(BASE_DIR, "TestCases", "logincase")
IMSCASE_DIR = os.path.join(BASE_DIR, "TestCases", "imscase")

# 日志目录
LOG_DIR = os.path.join(BASE_DIR, "OutPuts", "logs")
#截图目录
IMAGES_DIR = os.path.join(BASE_DIR, "OutPuts", "images")

# 测试数据文件
TEST_DATA = os.path.join(BASE_DIR, "TestDatas")
TEST_GOBAL_DATA = os.path.join(BASE_DIR, "TestDatas", "GobalDatas")
TEST_LOGIN_DATA = os.path.join(BASE_DIR, "TestDatas", "LoginDatas")
TEST_HOME_DATA = os.path.join(BASE_DIR, "TestDatas", "HomeDatas")
TEST_IMS_DATA = os.path.join(BASE_DIR, "TestDatas", "ImsDatas")

# 元素控件
TEST_Element = os.path.join(BASE_DIR, "Elements")

TEST_Trade_Element= os.path.join(BASE_DIR, "Elements", "TradeElements")

# 测试报告目录
TEST_REPORT = os.path.join(BASE_DIR, "Result")

# 驱动目录
CHROME_PATH= os.path.join(BASE_DIR, "Tools","chromedriver.exe")
FIREFOX_PATH= os.path.join(BASE_DIR, "Tools","geckodriver.exe")
IE_PATH= os.path.join(BASE_DIR, "Tools","IEDriverServer.exe")

#ExcelDatas目录
EXCEL_DATA = os.path.join(BASE_DIR, "ExcelDatas")
Trade_EXCEL=os.path.join(EXCEL_DATA,"trade.xls")
IMS_EXCEL=os.path.join(EXCEL_DATA, "ImsDatas", "home.xls")

