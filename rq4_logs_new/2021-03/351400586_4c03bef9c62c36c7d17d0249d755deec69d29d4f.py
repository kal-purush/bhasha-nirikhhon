import config
from selenium import webdriver
import selenium.common.exceptions as selexcept
import time

class Bot:

    def order_from_footlocker(self):
        driver = webdriver.Chrome('./bin/chromedriver') 
        options = driver.create_options()
        options.headless = False

        driver.get(config.keys['product_url'])
        time.sleep(5)
        try:
            driver.find_element_by_xpath('/html/body/div[3]/div[2]/div[4]/div[2]').click() #cookies
        except selexcept.ElementClickInterceptedException: #browser still loading
            time.sleep(5)
        driver.find_element_by_xpath('//*[@id="add-to-cart-form"]/div/div[2]/div/div[3]/div/div/div/span[1]').click() #select size
        time.sleep(.5)
        driver.find_element_by_xpath('//*[@id="fitanalytics_sizecontainer"]/section[2]/div/button[9]').click() #size 
        time.sleep(.5)
        driver.find_element_by_class_name('fl-btn__primary').click() #add_cart
        time.sleep(1)
        try:
            driver.find_element_by_xpath('//*[@id="flcomponentheaderfull"]/div/div[1]/span[1]/div/div[2]/div/div[2]/div[7]/div[1]/a').click() #buy_now
        except selexcept.NoSuchElementException:
            driver.get(config.keys['site_cart'])
        time.sleep(3)
        driver.find_element_by_xpath('/html/body/main/div/div[2]/div[1]/div[2]/div/div/div/a/span').click() #new_sign_in
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="billing_FirstNamecheckout-billing-address-form"]').send_keys(config.keys['fname'])
        driver.find_element_by_xpath('//*[@id="billing_LastName"]').send_keys(config.keys['lname'])
        #terms
        driver.find_element_by_xpath('').click()
        #dropdown ex
        driver.find_element_by_xpath('')

    def order_from(self):
        driver = webdriver.Chrome('./bin/chromedriver') 