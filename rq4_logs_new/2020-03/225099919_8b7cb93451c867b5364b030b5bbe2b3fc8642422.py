from appium import webdriver
import os,time

def startUp():

    desire_caps={
        "deviceName":"245576a2",
        "platformName":"android",
        "platfromVersion":"10",
        "appPackage":"com.ss.android.article.news",
        "appActivity":"com.ss.android.article.news.activity.MainActivity",
        "noReset":True,
        "unicodeKeyboard":True
    }

    driver = webdriver.Remote("http://127.0.0.1:4723/wd/hub",desire_caps)
    time.sleep(10)


    driver.find_element_by_id("com.ss.android.article.news:id/bth").click()

    #driver.find_element_by_android_uiautomator('new UiSelector().text(\"发微头条\")')[0].click()

    list1 = driver.page_source
    print(type(list1))
    print(list1)
    a = "发微头条"


   # //android.widget.LinearLayout['@id="com.ss.android.article.news:id/btg"']/android.widget.TextView
    if a in list1:
        print('存在')
    driver.find_element_by_id("com.ss.android.article.news:id/i4").click()



if __name__ == '__main__':
    startUp()