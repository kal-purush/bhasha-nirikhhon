import unittest
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class test_EmployeeLogin(unittest.TestCase):

   def setUp(self):
       self.driver = webdriver.Chrome("C:\\chromedriver.exe")

   def test_EmployeeLogin(self):
       user = "instructor"
       pwd = "test1234"
       driver = self.driver
       driver.maximize_window()
       driver.get("http://gs-food-pantry.herokuapp.com/admin/")
       elem = driver.find_element_by_id("id_username")
       elem.send_keys(user)
       elem = driver.find_element_by_id("id_password")
       elem.send_keys(pwd)
       elem.send_keys(Keys.RETURN)
       driver.find_element_by_xpath("//*[@id='content-main']/div[3]/table/tbody/tr[3]/td[1]/a").click()
       emp_number = "7"
       name = "Anuja Janet"
       address = "1110 S 67th st"
       city = "Omaha"
       state = "1110 S 67th st"
       zipcode = "68022"
       email = "ajanet@gmail.com"
       cell_phone = "1231234567"
       elem = driver.find_element_by_id("id_emp_number")
       elem.send_keys(emp_number)
       time.sleep(2)
       elem = driver.find_element_by_id("id_name")
       elem.send_keys(name)
       time.sleep(2)
       elem = driver.find_element_by_id("id_address")
       elem.send_keys(address)
       time.sleep(2)
       elem = driver.find_element_by_id("id_city")
       elem.send_keys(city)
       time.sleep(2)
       elem = driver.find_element_by_id("id_state")
       elem.send_keys(state)
       time.sleep(2)
       elem = driver.find_element_by_id("id_zipcode")
       elem.send_keys(zipcode)
       time.sleep(2)
       elem = driver.find_element_by_id("id_email")
       elem.send_keys(email)
       time.sleep(2)
       elem = driver.find_element_by_id("id_cell_phone")
       elem.send_keys(cell_phone)
       time.sleep(2)
       driver.find_element_by_xpath("//*[@id='employee_form']/div/div/input[1]").click()
       time.sleep(2)
       assert "Employee added"
       time.sleep(5)

   def tearDown(self):
       self.driver.close()

if __name__ == "__main__":
   unittest.main()