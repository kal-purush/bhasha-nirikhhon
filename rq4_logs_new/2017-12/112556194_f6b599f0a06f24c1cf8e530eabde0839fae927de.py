import unittest
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class test_EmployeeLogin(unittest.TestCase):

   def setUp(self):
       self.driver = webdriver.Chrome("C:\\chromedriver.exe")

   def test_EmployeeLogin(self):
       user = "instructor"
       pwd = "instructor1a"
       driver = self.driver
       driver.maximize_window()
       driver.get("http://gs-food-pantry.herokuapp.com/portfolio/login/?next=/")
       elem = driver.find_element_by_id("id_username")
       elem.send_keys(user)
       elem = driver.find_element_by_id("id_password")
       elem.send_keys(pwd)
       elem.send_keys(Keys.RETURN)
       driver.get("http://gs-food-pantry.herokuapp.com/portfolio/login/?next=/")
       assert "Logged In"
       time.sleep(5)

   def tearDown(self):
       self.driver.close()

if __name__ == "__main__":
   unittest.main()