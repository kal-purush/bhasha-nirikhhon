#!/usr/bin/python
import time
import urllib2
import thread
from datetime import datetime

#Configuration stuff
serverIP = "http://server.ip.goes.here:port"
#End configuration

def openURL(url):
	try:
		urllib2.urlopen(url)
		print "Sent URL" + url
	except:
		print "Error opening URL"

squidLogPath = "/var/log/squid3/access.log"
squidLog = open(squidLogPath)
squidLog.seek(0,2)
while True:
	line = squidLog.readline()
	if not line:
		time.sleep(0.1)
	else:
		line = line.lower()
		lineParts = line.split()
		#print lineParts
		fullURL = lineParts[6]
		fullURL = fullURL.replace("http://", "")
		urlToSend = fullURL.split("/")[0]
		try:		
			thread.start_new_thread(openURL, (serverIP+"/message?message="+urlToSend,))
		except:
			print "Error starting thread"
		