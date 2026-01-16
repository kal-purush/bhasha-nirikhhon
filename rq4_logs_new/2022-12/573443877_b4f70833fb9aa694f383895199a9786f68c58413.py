#!/usr/bin/python3
import sys
from pynput import keyboard

def on_press(key):
	try:
		print('{0}'.format(key.char), end='')
		sys.stdout.flush()
	except AttributeError:
		global line, lines
		if key==keyboard.Key.pause:
			print('>>> ', end='')
			for key in lines[line]:
				sender.press(key)
				sender.release(key)
			print()
			line+=1

			# ENTER can be sent:
			#sender.press(keyboard.Key.enter)
			#sender.release(keyboard.Key.enter)
		else:
			print('[{0}]'.format(key))

# INIT --------------------------------------------------------------------------
with open('commandlist') as file: lines=[line.rstrip() for line in file]
print("LINES READ:", lines)
line=0

sender=keyboard.Controller()
with keyboard.Listener(on_press=on_press) as listener: listener.join()