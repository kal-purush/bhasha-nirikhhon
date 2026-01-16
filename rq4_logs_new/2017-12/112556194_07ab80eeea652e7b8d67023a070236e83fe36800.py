import unittest
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class GS_Test(unittest.TestCase):

   def setUp(self):
       self.driver = webdriver.Chrome()


# Test Script 1 - Add New Client
   def test_add_client(self):
       user = "instructor"
       pwd = "instructor1a"
       driver = self.driver
       driver.maximize_window()
       driver.get("http://127.0.0.1:8000/portfolio/login/?next=/")
       elem = driver.find_element_by_id("id_username")
       elem.send_keys(user)
       elem = driver.find_element_by_id("id_password")
       elem.send_keys(pwd)
       time.sleep(4)
       elem.send_keys(Keys.RETURN)
       driver.get("http://127.0.0.1:8000")
       assert "Logged In"
       time.sleep(5)
       driver.get("http://127.0.0.1:8000/client/")
       time.sleep(10)
       elem = driver.find_element_by_xpath("//*[@id='app-layout']/div/div/div/div[3]/div/a").click()
       time.sleep(10)
       elem = driver.find_element_by_id("id_name")
       elem.send_keys("Monica Geller")
       elem = driver.find_element_by_id("id_address")
       elem.send_keys("1234 Pine Street")
       elem = driver.find_element_by_id("id_client_number")
       elem.send_keys("456789")
       elem = driver.find_element_by_id("id_city")
       elem.send_keys("Omaha")
       elem = driver.find_element_by_id("id_state")
       elem.send_keys("Nebraska")
       elem = driver.find_element_by_id("id_zipcode")
       elem.send_keys("68114")
       elem = driver.find_element_by_id("id_email")
       elem.send_keys("monica@gmail.com")
       elem = driver.find_element_by_id("id_cell_phone")
       elem.send_keys("7867895678")
       time.sleep(5)
       elem = driver.find_element_by_xpath("//*[@id='app-layout']/div/div/div/div/div/div/div/div/form/div[9]/div/button").click()
       time.sleep(10)
       assert "Added New Client"


   def tearDown(self):
           self.driver.close()

if __name__ == "__main__":
       unittest.main()