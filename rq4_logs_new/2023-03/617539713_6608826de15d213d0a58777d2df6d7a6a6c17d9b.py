import schedule
import time as tm
from datetime import datetime, timedelta, datetime

def sleep():
    print("Good Night")

def morning():
    print("Good Morning")

def family_meeting():
    print("Family Meeting")


schedule.every().day.at("06:00").do(morning)
schedule.every().day.at("21:14").do(sleep)
schedule.every().week.friday.do(family_meeting)

while True:
    schedule.run_pending()
    tm.sleep(1)