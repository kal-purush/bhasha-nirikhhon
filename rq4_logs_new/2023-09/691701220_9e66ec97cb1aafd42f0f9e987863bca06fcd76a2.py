# imports

from selenium import webdriver                                                          # To automate web browser we use a web driver
from selenium.webdriver.common.by import By                                             # as the (by=) is deprecated in selenium By has to imported
from time import sleep                                                                  # to avoid script from crashing due to internet speed we use sleep from time library to execute the next line with a delay
import os

# keeping Chrome browser open after program finishes

chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("detach", True)

# Setting up Login Variables

EMAIL = os.environ["EMAIL"]                                                             # Using Environment Variable Email
PASSWORD = os.environ["PASSWORD"]                                                       # Using Environment Variable Password

# Setting up driver

driver = webdriver.Chrome(options=chrome_options)                                       # Using chrome for running automation script

# Locating Target Web App

driver.get("https://www.browserstack.com/users/sign_in")

# Logging In

sleep(2)                                                                                # giving a delay of 2 seconds for the site to load properly before executing further commands
email_entry = driver.find_element(By.XPATH, '//*[@id="user_email_login"]')              # storing email entry as a variable
password_entry = driver.find_element(By.XPATH, '//*[@id="user_password"]')              # storing password entry as a variable
sign_in_button = driver.find_element(By.XPATH, '//*[@id="user_submit"]')                # storing sign in button as a variable
email_entry.send_keys(EMAIL)                                                            # sending email details in email entry
password_entry.send_keys(PASSWORD)                                                      # sending password details in password entry
sign_in_button.click()                                                                  # clicking the sign in button


# driver.quit()                                                                         # This Command exits the browser