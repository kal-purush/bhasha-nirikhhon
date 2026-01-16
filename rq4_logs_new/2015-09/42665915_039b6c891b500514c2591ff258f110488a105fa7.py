#!/usr/bin/env python
# -*- coding:utf-8 -*-

import socket, socks
import requests, time

from stem import Signal
from stem.control import Controller

#Obtain ip address.
URL_SUBMIT="http://icanhazip.com/"

TOR_CONTROLLER_PASS='password' #Change by your password

#Use Tor for any connections.
socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9050, True)

#Class socket by default.
SOCK_DEFAULT=socket.socket
#Class socket for Tor
SOCK_TOR=socks.socksocket

def my_ip():
	global URL_SUBMIT, SOCK_TOR
	socket.socket=SOCK_TOR
	
	r=requests.get(URL_SUBMIT)
	return (r.content).decode("utf-8")[:-1]
	
	
def new_ip():
	global SOCK_DEFAULT, TOR_CONTROLLER_PASS
	socket.socket=SOCK_DEFAULT
	changed=True
	msg='[OK]'
	
	try:
		controller=Controller.from_port(port=9051)
		controller.authenticate(password=TOR_CONTROLLER_PASS)
		controller.signal(Signal.NEWNYM)
	
	except Exception as err:
		changed=False
		msg=err
		
	time.sleep(2)
	return changed, msg
	
	
if __name__ == '__main__':		
	print("### TOR WITH PYTHON ###")
	
	result=my_ip()
	print('+[IP] %s'%result)

	modified, msg=new_ip()
	if modified:
		result=my_ip()
		print('+[IP] %s'%result)
	else:
		print(msg)