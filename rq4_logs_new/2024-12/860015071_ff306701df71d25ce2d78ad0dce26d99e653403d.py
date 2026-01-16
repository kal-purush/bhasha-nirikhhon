
from selenium.webdriver.common.by import By
from selenium import webdriver
from time import sleep

webdriver = webdriver.Chrome()
webdriver.get("<https://UserName:Password@qauto2.forstudy.space>;") # логін не працює
sleep(5)

sign_up_button = webdriver.find_element(by=By.XPATH, value='//*[text()="Sign up"]')
sign_in_button = webdriver.find_element(by=By.XPATH, value='//button[@class="btn btn-outline-white header_signin"]')
guest_log_in_button = webdriver.find_element(by=By.XPATH, value='//app-header/header//button[text()="Guest log in"]')
about_button = webdriver.find_element(by=By.XPATH, value='//app-header/header//button[text()="About"]')
contacts_button = webdriver.find_element(by=By.XPATH, value='//app-header/header//button[text()="Contacts"]')
home_button = webdriver.find_element(by=By.XPATH, value='//app-header/header//*[@href="/" and text()="Home"]')
hillel_auto_header_link = webdriver.find_element(by=By.XPATH, value='//app-header/header//*[@xmlns="http://www.w3.org/2000/svg"]/..')
video_title = webdriver.find_element(by=By.XPATH, value='//div[@class="ytp-title-text"]/a[contains(@class, "yt-uix-sessionlink")]')
support_hillel_link = webdriver.find_element(by=By.XPATH, value='//a[contains(@class, "contacts_link") and contains(. , "support")]')
footer_home_link = webdriver.find_element(by=By.XPATH, value='//a[@href="https://ithillel.ua"]')
footer_contacts_block = webdriver.find_element(by=By.XPATH, value='//div/h2[text()="Contacts"]')
footer_contacts_facebook_link = webdriver.find_element(by=By.XPATH, value='//a[@href="https://www.facebook.com/Hillel.IT.School"]')
footer_contacts_telegram_link = webdriver.find_element(by=By.XPATH, value='//a[@href="https://t.me/ithillel_kyiv"]')

footer_contacts_section = webdriver.find_element(by=By.CSS_SELECTOR, value='#contactsSection')
footer_contacts_instagram_link = webdriver.find_element(by=By.CSS_SELECTOR, value='a[href*=instagram]')
footer_contacts_youtube_link = webdriver.find_element(by=By.CSS_SELECTOR, value='a[href*="youtube"].socials_link')
footer_contacts_linkedin_link = webdriver.find_element(by=By.CSS_SELECTOR, value='a[href*="linkedin"].socials_link')
header_sign_in_button = webdriver.find_element(by=By.CSS_SELECTOR, value='.header .header_signin')
header_guest_log_in_button = webdriver.find_element(by=By.CSS_SELECTOR, value='.header .-guest')
sign_up_button = webdriver.find_element(by=By.CSS_SELECTOR, value='.section.hero button')
youtube_video = webdriver.find_element(by=By.CSS_SELECTOR, value='#player .ytp-title a[href*=youtube]')
support_hillel_link = webdriver.find_element(by=By.CSS_SELECTOR, value='a.contacts_link[href^="https"]')
video_title = webdriver.find_element(by=By.CSS_SELECTOR, value='.ytp-title-text .yt-uix-sessionlink')
about_button = webdriver.find_element(by=By.CSS_SELECTOR, value='.btn.header-link[appscrollto=aboutSection]')
contacts_button = webdriver.find_element(by=By.CSS_SELECTOR, value='.btn.header-link[appscrollto=contactsSection]')
header_logo_link = webdriver.find_element(by=By.CSS_SELECTOR, value='.header_logo[href="/"]')
home_button = webdriver.find_element(by=By.XPATH, value='nav [href="/"]')



webdriver.close()