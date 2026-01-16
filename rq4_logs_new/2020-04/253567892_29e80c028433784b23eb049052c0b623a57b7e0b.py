from logging import Formatter, getLogger, StreamHandler, INFO
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


def get_logger():
    logger = getLogger()
    formatter = Formatter("%(asctime)-15s - %(levelname)-8s - %(message)s")
    handler = StreamHandler()
    handler.setLevel(INFO)
    handler.setFormatter(formatter)
    logger.setLevel(INFO)
    logger.addHandler(handler)

    return logger


def open_browser():
    options = Options()
    options.add_argument("--disable-extensions")
    options.add_argument("--start-maximized")

    browser = webdriver.Remote(
        command_executor="http://selenium-hub:4444/wd/hub",
        desired_capabilities=DesiredCapabilities.CHROME,
        options=options,
    )
    browser.implicitly_wait(10)

    return browser


def login_oddspark(browser):
    user_id = os.getenv("ODDSPARK_USER_ID")
    password = os.getenv("ODDSPARK_PASSWORD")
    pin = os.getenv("ODDSPARK_PIN")

    browser.get("https://www.oddspark.com/keiba/")
    browser.find_element(By.CSS_SELECTOR, "body")
    browser.find_element(By.NAME, "SSO_ACCOUNTID").click()
    browser.find_element(By.NAME, "SSO_ACCOUNTID").send_keys(user_id)
    browser.find_element(By.NAME, "SSO_PASSWORD").click()
    browser.find_element(By.NAME, "SSO_PASSWORD").send_keys(password)
    browser.find_element(By.CSS_SELECTOR, "form > a").click()

    browser.find_element(By.NAME, "INPUT_PIN").click()
    browser.find_element(By.NAME, "INPUT_PIN").send_keys(pin)
    browser.find_element(By.NAME, "送信").click()

    browser.find_element(By.CSS_SELECTOR, ".modalCloseImg").click()


def is_logined_oddspark(browser):
    browser.get("https://www.oddspark.com/user/my/Index.do")
    browser.find_element(By.CSS_SELECTOR, "body")

    title = browser.title

    if title.startswith("ログイン"):
        return False
    else:
        return True