import logging
import time
import random
import requests
import os
import atexit

from datetime import datetime
from pytz import timezone

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from twilio.rest import Client

URL = "https://www.amd.com/en/direct-buy/ca"
SELECTOR = "#block-amd-content > div > div > div > div > div"
NAME_SELECTOR = "div.shop-content > div.direct-buy > div.shop-title"
BUTTON_SELECTOR = "div.shop-content > div.direct-buy > div.shop-links > button"
BUTTON_EXPECTED_TEXT = "Add to cart"
wd = 0

# Logging Config
logging.basicConfig(filename='tracker.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

# Set Paramaters
account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']
target_phone = os.environ['TARGET_PHONE']
sender_phone = os.environ['SENDER_PHONE']

if account_sid == None or auth_token == None:
    logging.error("Failed to fetch required environment variables!")
    quit()

client = Client(account_sid, auth_token)

def send_msg(message):
    msg =client.messages.create(body="AMD Check \n" + message, from_=sender_phone, to=target_phone)
    logging.info("Text message sent - " + str(msg.sid))

def start_chrome_driver():
    logging.info("Starting ChromeDriver.....")
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument('disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75")
    options.add_experimental_option( "prefs",{'profile.managed_default_content_settings.javascript': 2})
    options.add_experimental_option( "prefs",{'profile.managed_default_content_settings.images': 2})
    options.add_argument('--disable-dev-shm-usage')
    wd1 = webdriver.Chrome("/usr/bin/chromedriver",options=options)
    wd1.set_page_load_timeout(15)
    logging.info("ChromeDriver Started!")
    return wd1

def exit_handler():
    if wd != 0:
        wd.quit()
        logging.info("Closing chromedriver")

atexit.register(exit_handler)

if __name__ == "__main__":
    wd = start_chrome_driver()
    logging.info("Sending start message!")
    send_msg("Chrome Driver Started")

    product_info = []
    shift_send = True
    shift_send_end = False

    while True:
        now_time = datetime.now(timezone('US/Eastern'))
        message = ""
        trigger_send = False

        if now_time.hour == 0 and now_time.minute == 0:
            send_msg("Script still running!")
            time.sleep(60)

        if now_time.hour >= 8 and now_time.hour <= 18:
            if shift_send:
                send_msg("starting day check")
                shift_send = False
                shift_send_end = True
                
            try:
                wd.get(URL)
            except Exception as err:
                logging.warning(err)
                continue

            try:
                elements = wd.find_elements(By.CSS_SELECTOR, SELECTOR)
            except Exception as err:
                logging.warning(err)
                continue

            for elem in elements:
                try:
                    item = elem.find_element(By.CSS_SELECTOR, NAME_SELECTOR)
                    prod_name = item.text.strip()

                    if [prod_name, False or True] not in product_info:
                        product_info.append([prod_name, False])

                    index = 0
                    for i in range(0, len(product_info)):
                        if product_info[i][0] == prod_name:
                            index = i
                            break
                        if i == len(product_info) - 1:
                            logging.error("Product not found in list, quitting!")
                            quit()

                    try:
                        stock_check = elem.find_element(By.CSS_SELECTOR, BUTTON_SELECTOR)
                        if not product_info[index][1]:
                            product_info[index][1] = True
                            trigger_send = True
                            message += product_info[index][0] + "\nIn Stock!\n"
                        logging.info(product_info[index][0] + " In Stock!")

                    except Exception as err:
                        product_info[index][1] = False
                        logging.info(product_info[index][0] + " Out of Stock!")

                except Exception as err:
                    logging.warning(err)

            if trigger_send:
                send_msg(URL + " \n" + message)
        else:
            if shift_send_end:
                send_msg("ending day check")
                shift_send_end = False
                shift_send = True

        timeout = random.randint(5, 10)
        logging.info("Sleep for " + str(timeout) + "s")
        time.sleep(timeout)