import chardet
import csv
import os
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Make sure you are ready for 2FA authentication.

music_list = []
options = webdriver.ChromeOptions()
options.add_argument(r'--user-data-dir=C:\\Users\\harshpatel\\AppData\\Local\\Google\\Chrome\\User Data')
options.add_argument("--profile-directory=Default")

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)

ELEMENT_SIGN_IN = "/html/body/ytmusic-app/ytmusic-app-layout/ytmusic-nav-bar/div[3]/a"
ELEMENT_SEARCH_BOX = "/html/body/ytmusic-app/ytmusic-app-layout/ytmusic-nav-bar/div[2]/ytmusic-search-box/div/div[1]/input"
ELEMENT_FILTER_SONG = "/html/body/ytmusic-app/ytmusic-app-layout/div[4]/ytmusic-search-page/ytmusic-tabbed-search-results-renderer/div[2]/ytmusic-section-list-renderer/div[1]/ytmusic-chip-cloud-renderer/iron-selector/ytmusic-chip-cloud-chip-renderer[1]/div/a"
# ELEMENT_MORE =  "/html/body/ytmusic-app/ytmusic-app-layout/div[4]/ytmusic-search-page/ytmusic-tabbed-search-results-renderer/div[2]/ytmusic-section-list-renderer/div[2]/ytmusic-card-shelf-renderer/div[2]/div[2]/div[2]/ytmusic-menu-renderer/yt-button-shape/button"
# ELEMENT_MORE = "/html/body/ytmusic-app/ytmusic-app-layout/div[4]/ytmusic-search-page/ytmusic-tabbed-search-results-renderer/div[2]/ytmusic-section-list-renderer/div[2]/ytmusic-shelf-renderer/div[3]/ytmusic-responsive-list-item-renderer[1]/ytmusic-menu-renderer/yt-button-shape/button"
ELEMENT_FIRST_SONG = "/html/body/ytmusic-app/ytmusic-app-layout/div[4]/ytmusic-search-page/ytmusic-tabbed-search-results-renderer/div[2]/ytmusic-section-list-renderer/div[2]/ytmusic-shelf-renderer/div[3]/ytmusic-responsive-list-item-renderer[1]/div[2]/div[1]/yt-formatted-string/a"
ELEMENT_SAVE = "/html/body/ytmusic-app/ytmusic-popup-container/tp-yt-iron-dropdown/div/ytmusic-menu-popup-renderer/tp-yt-paper-listbox/ytmusic-toggle-menu-service-item-renderer[1]"
ELEMENT_SAVE_TEXT = "/html/body/ytmusic-app/ytmusic-popup-container/tp-yt-iron-dropdown/div/ytmusic-menu-popup-renderer/tp-yt-paper-listbox/ytmusic-toggle-menu-service-item-renderer[2]/yt-formatted-string"

def clear_screen():
  os.system("cls")

def parse_csv(file_path):
  def detect_encoding(file_path):
      with open(file_path, 'rb') as file:  # Read the file in binary mode
          result = chardet.detect(file.read())
          return result['encoding']

  encoding = detect_encoding(file_path)
  print(f"Detected encoding: {encoding}")

  with open(file_path, encoding=encoding) as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    next(reader, None)  # skip header
    for row in reader:
      music_list.append(" - ".join(row))
    
    music_list.reverse()
    for i, music in enumerate(music_list):
       print(f"{i}. {music}")

class App():
  csv_file_path = ""

  def __init__(self):
    self.main()
  

  def main(self):
      print("A managed instance of Google Chrome has started, do not close it.\nMake sure you are already logged in on your default Chrome Profile to a Google Account\nYou will not be able to login to this browser as it is not trusted by Google")
      
      driver.get("https://music.youtube.com")

      self.import_csv()
      
      input("Press ENTER to start, then maximize the Managed Chrome Window. You are free to minimize it after maximizing it.\nYou have 10 seconds to do so.")
      self.init()
      clear_screen()
      self.start()
      

  def import_csv(self):
    print("Drag the exported Spotify CSV file to the terminal and press ENTER.")
    self.csv_file_path = input("File: ")
    
    clear_screen()
    
    print("Importing File")
    parse_csv(self.csv_file_path)
    
    print("Your music has been imported, you can check to see if all your songs have been imported.")
    print(f"Total Count: {len(music_list)}")
    print("Press ENTER to confirm and continue")
    input()
    
    return None
  
  def init(self):
    time.sleep(10)
    clear_screen()
    print("Reverting to Home Screen")
    driver.get("https://music.youtube.com")

  def start(self):
      for i, music in enumerate(music_list):
        print(f"{i+1}/{len(music_list)}. Attempting to add {music} to liked songs:")
        print(f"    Searching for {music}")
        try: 
          driver.find_element(By.XPATH, ELEMENT_SEARCH_BOX).click()
          driver.find_element(By.XPATH, ELEMENT_SEARCH_BOX).clear()

          driver.find_element(By.XPATH, ELEMENT_SEARCH_BOX).send_keys(music)
          driver.find_element(By.XPATH, ELEMENT_SEARCH_BOX).send_keys(Keys.ENTER)

          time.sleep(1)
          print("    Filtering Search for Songs only")
          driver.find_element(By.XPATH, ELEMENT_FILTER_SONG).click()
          time.sleep(1)

          first_song_element = driver.find_element(By.XPATH, ELEMENT_FIRST_SONG)
          actions = ActionChains(driver)
          

          # Right-click on the element (context click)
          print("    Opening Right Click Context Menu of First Result")
          actions.context_click(first_song_element).perform()

          if driver.find_element(By.XPATH, ELEMENT_SAVE_TEXT).text.strip() == "Remove from liked songs":
            print("    This song is already liked, skipping\n\n")
            continue

          time.sleep(1)
          print("    Clicking Add to Liked Songs")
          driver.find_element(By.XPATH, ELEMENT_SAVE_TEXT).click()
          print(f"    Added {music} to Liked Songs")
          print("\n")


        except Exception as e:
          print(e)
          print(f"    Adding {music} failed, going back to homepage and moving on.")
          driver.get("https://music.youtube.com")
          continue

App()