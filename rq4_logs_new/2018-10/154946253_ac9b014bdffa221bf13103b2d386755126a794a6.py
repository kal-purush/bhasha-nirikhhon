# from apscheduler.scheduler import Scheduler
#
# sched = Scheduler()
# sched.start()
#
# def some_job():https://www.hotstar.com/sports/cricket/unimoni-asia-cup-2018/india-vs-pakistan-m187575/live-streaming/2001707108
#     print "Every 10 seconds"
#
# sched.add_interval_job(some_job, seconds = 10)
#
# ....
# sched.shutdown()

import time
from selenium import webdriver
url='https://www.hotstar.com/sports/cricket/unimoni-asia-cup-2018/india-vs-bangladesh-m187583/live-streaming/2001707196'

def init():
    fp = webdriver.FirefoxProfile()
    # fp.set_preference("network.cookie.cookieBehavior", 2)
    browser = webdriver.Firefox(
        firefox_profile=fp,
        executable_path="C:\\Users\\Junaid\\Downloads\\geckodriver-v0.20.1-win64\\geckodriver.exe")
    browser.get(url)
    # browser.find_element_by_xpath('/html/body/div[1]/div/div/div/div[1]/div[1]/div/div[3]/div/div/div/div/div[2]/div[4]/button[2]').click()
    time.sleep(20)
    print("clicking ..")
    browser.find_element_by_css_selector('.vjs-fullscreen-control.vjs-control.vjs-button').click()
    time.sleep(540)
    executeSomething(browser)


def executeSomething(browserOld):
    fp = webdriver.FirefoxProfile()
    # fp.set_preference("network.cookie.cookieBehavior", 2)
    browser = webdriver.Firefox(
        firefox_profile=fp,
        executable_path="C:\\Users\\Junaid\\Downloads\\geckodriver-v0.20.1-win64\\geckodriver.exe")
    browser.get(url)
    # browser.find_element_by_xpath("/html/body/div[1]/div/div/div/div[1]/div[1]/div/div[3]/div/div/div/div/div[2]/div[4]/button[2]").click()
    time.sleep(20)
    browser.find_element_by_css_selector('.vjs-fullscreen-control.vjs-control.vjs-button').click()
    browserOld.quit()
    time.sleep(240)
    executeSomething(browser)


init()