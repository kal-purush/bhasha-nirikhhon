from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
import pandas as pd
import pymysql

# create a new Firefox session
driver = webdriver. Chrome()
driver.implicitly_wait(30)
driver.maximize_window()

url = 'http://cctv.dlink.co.in/'
driver.get(url)
driver.implicitly_wait(30)

record = []

list_link = ['AHD Cameras', 'Digital Video Recorders (DVR)','CCTV IP Cameras', 'Network Video Recorders (NVR)', 'IP Cameras', 'IP Pan Tilt & Zoom Cameras (PTZ)', 'Network Video Recorders (NVR)', 'Thermal Solution']

for ddlink in list_link:
    print("Product categories name :",ddlink)
    Product_categories_name = ddlink

    mouse_move = ActionChains(driver)

    mouse_move.move_to_element(driver.find_element_by_link_text('PRODUCTS')).perform()
    driver.implicitly_wait(20)

    driver.find_element_by_link_text(ddlink).click()
    driver.implicitly_wait(50)

    product_link_list = []
    product_link = driver.find_element_by_class_name('col-sm-8').find_elements_by_tag_name('a')
    for product in product_link:
        print (product.get_attribute("href"))
        link = product.get_attribute("href")
        product_link_list.append(link)

    print(product_link_list)

    for link_list in product_link_list:

        driver.get(link_list)

        product_image = driver.find_element_by_xpath('//*[@id="mainPic"]').get_attribute('src')
        print(product_image)

        table_list = []
        for tr in driver.find_elements_by_xpath('//*[@id="tabSpecs"]/table/tbody/tr'):
            tds = tr.find_elements_by_tag_name('td')
            if tds:
                table_list.append([td.text for td in tds])

        specification = pd.DataFrame(table_list)
        print(specification)


        all_records = (Product_categories_name,product_image, specification,  )

        record.append(all_records)
        print(record)

df = pd.DataFrame(record, columns=['Product_categories_name', 'product_image','specification' ])

export_into_excel_file = df.to_excel('ddlink_data.xlsx')
print('data save successfully........')
