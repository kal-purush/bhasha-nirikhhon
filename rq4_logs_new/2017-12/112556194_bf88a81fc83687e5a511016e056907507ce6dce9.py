import unittest
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class test_AdminLogin(unittest.TestCase):

   def setUp(self):
       self.driver = webdriver.Chrome("C:\\chromedriver.exe")

   def test_AdminLogin(self):
       user = "instructor"
       pwd = "test1234"
       driver = self.driver
       driver.maximize_window()
       driver.get("http://gs-food-pantry.herokuapp.com/admin")
       elem = driver.find_element_by_id("id_username")
       elem.send_keys(user)
       elem = driver.find_element_by_id("id_password")
       elem.send_keys(pwd)
       elem.send_keys(Keys.RETURN)
       assert "Logged In"
       driver.find_element_by_xpath("//*[@id='content-main']/div[2]/table/tbody/tr/th/a").click()
       driver.find_element_by_xpath("//*[@id='result_list']/tbody/tr[1]/td[12]/a").click()
       driver.find_element_by_xpath("//*[@id='content']/ul/li/a").click()
       driver.find_element_by_xpath("//*[@id='print-header']/div/button[1]").click()
       assert " Printed Receipt Successfully"
       time.sleep(5)

   def tearDown(self):
       self.driver.close()

if __name__ == "__main__":
   unittest.main()
