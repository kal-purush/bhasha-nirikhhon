#!/usr/bin/python
# coding: utf-8
#---------------------------
# Writed By WitBlack HAcker
#---------------------------
#💬 Telegram:
#Https://t.me/WitBlack_ch
#
#💻 Web:
#Https://BugZone.ir
#
#📹 YouTube:
#https://www.youtube.com/channel/UCIgk2ldVeelyaHW3s4UkIIg (WitBlack)
#
#🎥 Aparat:
#Https://aparat.com/WitBlack
#
#⌨️ Github:
#Https://github.com/WitBlack
#
#📧 E-Mail:
#admin@bugzone.ir
#
#💬 Telegram:
#Https://t.me/WitBlack_ch
#
#💻 Web:
#Https://BugZone.ir
#
#📹 YouTube:
#https://www.youtube.com/channel/UCIgk2ldVeelyaHW3s4UkIIg (WitBlack)
#
#🎥 Aparat:
#Https://aparat.com/WitBlack
#
#⌨️ Github:
#Https://github.com/WitBlack
#
#📧 E-Mail:
#admin@bugzone.ir
#Powered By WitBlack Hacker
#Version 1.0.2 - Meli Code Generator
#
#💬 Telegram:
#Https://t.me/WitBlack_ch
#
#💻 Web:
#Https://BugZone.ir
#
#📹 YouTube:
#https://www.youtube.com/channel/UCIgk2ldVeelyaHW3s4UkIIg (WitBlack)
#
#🎥 Aparat:
#Https://aparat.com/WitBlack
#
#⌨️ Github:
#Https://github.com/WitBlack
#
#📧 E-Mail:
#admin@bugzone.ir
#
try:
	import os
	from time import sleep
	from termcolor import colored
except:
	print("Erorr!\nSome deepends not installed!")
	exit(1)
Command = raw_input("Enter your command: ")
try:
	Time = int(raw_input(colored("Enter sleep by sec: ",'white')))
except:
	print(colored("Time number is invalid!",'yellow'))
	exit(1)
if os.path.exists('C:'):
	OS = 'Windows'
elif os.path.exists('/tmp/'):
	OS = 'Linux'
else:
	print(colored('Your Operation system not supported! :(','yellow'))
	exit(1)
def clear():
	if OS == 'Windwos':
		os.system('cls')
	else:
		os.system('clear')
while True:
	try:
		clear()
		print os.popen(Command).read()
		sleep(Time)
	except:
		print(colored("\nInvalid command or exit with client request.",'yellow'))
		exit(0)