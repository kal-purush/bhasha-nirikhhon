from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

# Keeping Chrome open
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("detach", True)

driver = webdriver.Chrome(options=chrome_options)
driver.get("https://en.wikipedia.org/wiki/Main_Page")

no_of_articles = driver.find_element(By.CSS_SELECTOR, value="#articlecount a")
# print(no_of_articles.text)


# Clicking on object
# no_of_articles.click()

all_portals = driver.find_element(By.LINK_TEXT, value="Content portals")
# all_portals.click()


search_field = driver.find_element(By.NAME, value="search")
search_field.send_keys("Python", Keys.ENTER)


# ----------------------- TEST -----------------------
driver2 = webdriver.Chrome(options=chrome_options)
driver2.get("https://secure-retreat-92358.herokuapp.com/")

first_name = driver2.find_element(By.NAME, value="fName")
first_name.send_keys("Rob")

last_name = driver2.find_element(By.NAME, value="lName")
last_name.send_keys("Ert")

email = driver2.find_element(By.NAME, value="email")
email.send_keys("fake@email.com")

sign_up = driver2.find_element(By.CSS_SELECTOR, value="button")
sign_up.send_keys(Keys.ENTER)

# driver.close()
# driver.quit()