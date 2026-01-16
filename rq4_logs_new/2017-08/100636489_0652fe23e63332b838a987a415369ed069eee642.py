__author__ = 'Zachary C Luscher' \

from selenium import webdriver
from time import sleep

import sys

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import xlrd
book = xlrd.open_workbook('List.xlsm')
sheet = book.sheet_by_name('Sheet1')

vals = []
for x in range(0,49):
    x1 = [sheet.col_values(colx=2, start_rowx=x, end_rowx=x+1), (sheet.col_values(colx=3, start_rowx=x, end_rowx=x+1))]
    vals.append(x1)

# opens netsuite and logs in

driver = webdriver.Chrome("C:\\Users\Blu\AppData\Local\Programs\Python\Python36\selenium\webdriver\chrome\chromedriver.exe")
driver.set_page_load_timeout(90)
driver.get("https://system.na1.netsuite.com/app/login/secure/loginrouter.nl?rdt=%2Fapp%2Faccounting%2Ftransactions%2Fsalesord.nl%3Fwhence%3D")
driver.maximize_window()
driver.implicitly_wait(1)
driver.find_element_by_name("email").send_keys("__________________email")
driver.find_element_by_id("password").send_keys("___________Pass")
driver.find_element_by_class_name("bgbutton").click()
driver.find_element_by_name("answer").send_keys("_______________auth")
driver.find_element_by_class_name("bgbutton").click()

sleep(1)


'''
element = driver.find_element_by_id("item_item_display")
element.send_keys(vals[0][0])
sleep(1)

driver.find_element_by_xpath('//*[@id="item_splits"]/tbody/tr[2]/td[4]').click()
sleep(2)
driver.switch_to.alert.accept()

driver.find_element_by_xpath('//*[@id=\"quantity_formattedValue\"]').send_keys(Keys.BACKSPACE, vals[0][1])
'''

# Part 1 Items 26-50 enters in.
x = 0
y = 0

while x in range(0, 26-y):
    if vals[x][0] == [42]:

        print("Now fill out the Customer data, Save the Sales Order, "
              "then just left of the document icon on the confirmation page. ")
        print("Highlight until the end of the Items list. example picture in NetsuiteAuto  (OracleCopy.png)")

        sys.exit()
    else:
        element = driver.find_element_by_id("item_item_display")
        element.send_keys(vals[x][0])
        sleep(0.2)

        driver.find_element_by_xpath('//*[@id="item_splits"]/tbody/tr[%s]/td[4]' %(x+y+2)).click()
        try:
            driver.find_element_by_xpath('//*[@id="inner_popup_div"]/table/tbody/tr[2]/td[2]/a').click()
        except:
            pass

        sleep(0.4)
        try:
            driver.switch_to.alert.accept()
        except:
            pass

        driver.find_element_by_xpath('//*[@id=\"quantity_formattedValue\"]').send_keys(Keys.BACKSPACE, vals[x][1])

        try:
            Backorder = driver.find_element_by_xpath('//*[@id="item_splits"]/tbody/tr[%s]/td[30]/div' %(x+y+2)).text
            Ordered = driver.find_element_by_xpath('//*[@id="item_splits"]/tbody/tr[%s]/td[31]/div' %(x+y+2)).text

            print(vals[x][1])
            print(vals[x][0])
            print(Backorder)
            print(Ordered)

            if Ordered == str(' ') :
                Ordered = str('100')
            else:
                Ordered = Ordered

            Backorder = int(Backorder)
            Ordered = int(Ordered)


            print(Backorder)
            print(Ordered)

            if Backorder - 1 >= Ordered:
                driver.find_element_by_xpath('//*[@id="item_clear"]').click()
                y -= 1
                print('Cancelled')
                print(" ")
            else:
                print('Success')
                driver.find_element_by_xpath('//*[@id="item_addedit"]').click()
                del Backorder
                del Ordered
        except:
            pass

    x += 1

# needs backordered and ordered added *********************************************************
# Part 2  Items 26-etc...
driver.find_element_by_xpath('//*[@id="item_splits"]/tbody/tr[27]').click()

for x in range(26, 50):
    if vals[x][0] == [42]:

        sys.exit()
    else:
        element = driver.find_element_by_id("item_item_display")
        element.send_keys(vals[x][0])
        sleep(0.2)
                                                                                                                
        driver.find_element_by_xpath('//*[@id="item_splits"]/tbody/tr[%s]/td[4]' %(x-23)).click()
        try:
            driver.find_element_by_xpath('//*[@id="inner_popup_div"]/table/tbody/tr[2]/td[2]/a').click()
        except:
            pass

        sleep(0.3)
        try:
            driver.switch_to.alert.accept()
        except:
            pass
        driver.find_element_by_xpath('//*[@id=\"quantity_formattedValue\"]').send_keys(Keys.BACKSPACE, vals[x][1])
        driver.find_element_by_xpath('//*[@id="item_addedit"]').click()

print("Now fill out the Customer data, Save the Sales Order, then just left of the document icon on the confirmation")
print("page. Highlight until the end of the Items list. example picture in Netsuite auto(OracleCopy.png)")