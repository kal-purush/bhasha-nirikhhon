# Scenario
# Open the default browser with url www.facebook.com
# Press-auto full screen button of the browser (usually F11)
# Login to Facebook is expected

# Make an dictionary in Python of URLs on Facebook of people I would like to see.
# The person s name is the key, the URL is the value
facebook_urls={'Dragan Georgiev':'https://www.facebook.com/dragan.georgiev.7','Dena Abova':'https://www.facebook.com/dena.abova','German Tepavicharov':'https://www.facebook.com/german.tepavicharov'}
photo_view_trigger='https://www.facebook.com/photo.php'
delay_between_photos=5

# Show the whole list of URLs
# Make a choice
# Go to chosen URL + suffix "/photos". This show persons photo album

# Needed software packages
# Python 2.7, Pillow, PyAutoGUI
import os
import webbrowser
import pyautogui
import time

# Open the default browser with url www.facebook.com
webbrowser.open(facebook_urls["Dragan Georgiev"])


# Pause of 3 seconds
time.sleep(3)

# Go to photo album
webbrowser.open(facebook_urls["Dragan Georgiev"]+"/photos")

# Pause of 3 seconds
time.sleep(3)

# Auto-press full screen button of the browser (usually F11)
pyautogui.press('F11')

# The program waits until the user chooses an initial photo form the album
# Then auto view mode (slide show) is started
# Pressing left arrow every 5 sec
while(True):
   pyautogui.press('right')
   time.sleep(delay_between_photos)
