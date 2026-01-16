#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tweepy
import json
import os.path
import time
from datetime import datetime
import sys


def log(l):
    with open("GilgameshBot/GilgameshBot.log", 'a') as lout:
        lout.write("{}: {}\n".format(str(datetime.now()), str(l)))


def get_time():
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())


if len(sys.argv) > 1:
    log("*****postponing run by {}s'*****".format(sys.argv[1]))
    time.sleep(int(sys.argv[1]))

with open("GilgameshBot/GilgameshBotSecrets.json", 'r') as fin:
    secrets = json.load(fin)

with open("GilgameshBot/heb_numbers.json", 'r') as fin:
    numbers = json.load(fin)

DELAY_IN_SEC = 1800
ERROR_DELAY = 60
DESCRIPTION_TEMPLATE = "מצייץ את עֲלִילוֹת גִּלְגָּמֶשׁ. עכשיו בלוח הָ{}. בוט בהשראת @%s מאת @%s" % (secrets["credit_maker"], secrets["credit_inspired"])
line_TEMPLATE = "{}\n\n~ לוּחַ {} שׁוּרָה {}."

log("*****Starting GilgameshBot*****")
now = get_time()


with open("GilgameshBot/parsed_gilgamesh.json", 'r') as fin:
    Gilgamesh = json.load(fin)
if os.path.isfile(secrets["state_path"]):
    with open(secrets["state_path"], 'r') as fin:
        state = json.load(fin)
        tablet = state["tablet"]
        line = state["line"]
        last_post_time = state["time"]
        log("Found recovering point!!!")
        time_diff = now - last_post_time
        log("time diff from last run is: {}sec.".format(str(time_diff)))
        if time_diff > DELAY_IN_SEC:
            log("Lagging continue immediately")
            print("Lagging continue immediately")
        else:
            log("Restored in fine delay, sleeping till next")
            time.sleep(DELAY_IN_SEC - time_diff)
else:
    log("No start point found restarting!!!")
    tablet = 0
    line = 0

auth = tweepy.OAuthHandler(consumer_key=secrets["consumer_key"], consumer_secret=secrets["consumer_secret"])
auth.set_access_token(secrets["access_token"], secrets["access_secret"])
api = tweepy.API(auth)

while True:
    try:
        if line == 0:
            api.update_profile(description=DESCRIPTION_TEMPLATE.format(Gilgamesh[tablet]["tablet_heb_ind"]))
        api.update_status(line_TEMPLATE.format(
            Gilgamesh[tablet]["lines"][line]["line_text"],
            Gilgamesh[tablet]["tablet_heb_ind"],
            numbers[line]
        ))
    except Exception as e:
        print("{}: ERROR - {} ".format(str(datetime.now()), str(e)))
        log("{}: ERROR - {} ".format(str(datetime.now()), str(e)))
        time.sleep(ERROR_DELAY)
    else:
        with open("GilgameshBot/heartbeat.log", 'w') as hbout:
            hbout.write('heartbeat at: {}, posted tablet: {}, line: {}.'.format(datetime.now(), str(tablet), str(line)))
        line = line + 1
        if len(Gilgamesh[tablet]["lines"]) == line:
            line = 0
            tablet = (tablet + 1) % 150
        with open(secrets["state_path"], 'w') as fout:
            json.dump({
                "tablet": tablet,
                "line": line,
                "time": get_time()
            }, fout)
        time.sleep(DELAY_IN_SEC)