import time

import pytest
from appium import webdriver
import logging

logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="module")
def base_driver():
    caps = {}
    caps["platformName"] = "android"
    caps["deviceName"] = "hogwarts"
    caps["appPackage"] = "com.xueqiu.android"
    caps["appActivity"] = ".view.WelcomeActivityAlias"
    caps["autoGrantPermissions"] = True
    driver = webdriver.Remote("http://localhost:4723/wd/hub", caps)
    yield driver
    logging.info("大家伙")
    time.sleep(10)
    driver.quit()
