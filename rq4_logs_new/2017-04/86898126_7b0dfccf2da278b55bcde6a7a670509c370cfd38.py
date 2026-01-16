#!/usr/bin/env python

'''
PiBuzz. btnagent lib
Webhook emitter test

@actuino
'''

import btnagent
import requests

WEBHOOK_URL = 'http://127.0.0.1/command'

def send_webhook_json(dic):
	requests.post(WEBHOOK_URL, json=dic)


@btnagent.click()
def btn_click(duration):
	print("Click",duration)
	try:
		send_webhook_json({"Command":"NextPage"})
	except Exception as e:
		print "Webhook Error ", str(e)

@btnagent.long_click()
def btn_long_click(duration):
	print("Long Click",duration)
	if duration == 1:
		print "Should reboot"
	if duration == 2:
		print "Shoud shutdown"

if __name__ == '__main__': 
	btnagent.loop()