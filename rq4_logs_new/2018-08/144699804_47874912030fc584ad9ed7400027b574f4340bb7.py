'''
Setup and Base Functions for all HID analytics
'''
import json
import urllib.request
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from pytz import timezone

# Set up Google Sheets connection
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
gc = gspread.authorize(credentials)
wks = gc.open("HID Metrics")

ACCESS_TOKEN = 'ACCESS_TOKEN'

def open_url(url):
    req = urllib.request.Request(url)
    r = urllib.request.urlopen(req).read()
    content = json.loads(r.decode('utf-8'))
    return content

def update_timestamp(worksheet): # Pull time of program execution and update
    geneva = timezone('Etc/GMT-2')
    current_time = datetime.now(geneva)
    formatted_time = current_time.strftime("%d %m %Y %H:%M:%S")
    updated = "As of: " + formatted_time + ' (GMT+2)'
    return updated