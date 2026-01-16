#!/usr/bin/python3
# -*- coding: utf-8 -*-

""" 
Docs for infi.systray -> https://github.com/Infinidat/infi.systray
"""
from infi.systray import SysTrayIcon

import datetime
import time


def date_progress(systray):
    time = datetime.datetime.now().time()
    time_seconds = float((int(time.hour)*3600) + (int(time.minute)*60) + int(time.second))
    perc_passed_day = "{:.2%}".format(float(time_seconds)/86400.0)
    print(perc_passed_day)
    return perc_passed_day


if __name__ == "__main__":

    menu_options = (("Say Hello", None, date_progress),)
    systray = SysTrayIcon("icon.ico", "Example tray icon", menu_options)
    systray.start()

    while True:
        perc_passed_day = "Today's progress: " + str(date_progress(systray))
        systray.update(hover_text = perc_passed_day)
        time.sleep(1)

    