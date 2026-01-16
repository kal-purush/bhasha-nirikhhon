from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from datetime import datetime
from dotenv import dotenv_values

#Get current time
now = datetime.now()
current_time = now.strftime("%H:%M")
config = dotenv_values(".env")
#  Code to allow access for Microphone, Camera and notifications
# 0 is disable and 1 is allow.
opt = Options()
opt.add_argument("--disable-infobars")
opt.add_argument("start-maximized")
opt.add_argument("--disable-extensions")
# Pass the argument 1 to allow and 2 to block
opt.add_experimental_option("prefs", { \
"profile.default_content_setting_values.media_stream_mic": 1, 
"profile.default_content_setting_values.media_stream_camera": 1,
"profile.default_content_setting_values.geolocation": 1, 
"profile.default_content_setting_values.notifications": 1 
})
    
# directing to the link to be visited; The program first logs into gmail for all around access of google services.
def connect():
    driver.get("https://accounts.google.com/ServiceLogin?service=mail&passive=true&rm=false&continue=https://mail.google.com/mail/&ss=1&scc=1&ltmpl=default&ltmplcache=2&emr=1&osid=1#identifier")
    time.sleep(4)
    driver.find_element_by_xpath("//input[@name='identifier']").send_keys(config["EMAIL"])
    time.sleep(2)
        # Next Button:
    driver.find_element_by_xpath("//*[@id='identifierNext']/div/button/div[2]").click()
    time.sleep(5)
        #Password:
    driver.find_element_by_xpath("//input[@name='password']").send_keys(config["PASSWORD"])
    time.sleep(2)
        #next button:
    driver.find_element_by_xpath("//*[@id='passwordNext']/div/button").click()
    time.sleep(5)
        # #opening Meet:
    driver.get(sub)
    driver.refresh()
    time.sleep(5)
        # Turning off video 
    driver.find_element_by_xpath("//*[@id='yDmH0d']/c-wiz/div/div/div[4]/div[3]/div/div[2]/div/div/div[1]/div[1]/div[3]/div[2]/div/div").click()
    time.sleep(1)
        # turning off audio
    driver.find_element_by_xpath("//*[@id='yDmH0d']/c-wiz/div/div/div[4]/div[3]/div/div[2]/div/div/div[1]/div[1]/div[3]/div[1]/div/div/div").click()
    time.sleep(1)
        # Join class
    driver.find_element_by_xpath("//*[@id='yDmH0d']/c-wiz/div/div/div[4]/div[3]/div/div[2]/div/div/div[2]/div/div[2]/div/div[1]/div[1]/span").click()

#Compare current time until the defined time
while current_time != config["TIME"]:
    if config["TIME"] == "":
        break
    time.sleep(60)
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    print(current_time)

#sub is the class id with the meet link. sub changes with the time accoriding to the class.
sub = config["MEET"]
driver = webdriver.Chrome(chrome_options=opt, executable_path=r'chromedriver') 
#you will need to change the executable_path=r'chromedriver' to the path where you have downloaded the chromedriver or any browerdrive. I used chromium for the test.

connect()  

print("No meets right now")