# coding=utf-8

import pynput, time, os, psutil
from pynput.mouse import Button
from pynput.keyboard import Key
mouse = pynput.mouse.Controller()
keyboard = pynput.keyboard.Controller()

def autoinput():
	mouse.position=(499, 273)
	mouse.click(Button.left)
	keyboard.press(Key.end)
	keyboard.release(Key.end)
	keyboard.press(Key.shift)
	keyboard.release(Key.shift)
	keyboard.press('.')
	keyboard.press('/')
	keyboard.press('y')
	keyboard.press('l')
	keyboard.press('c')
	keyboard.press('.')
	keyboard.press('s')
	keyboard.press('h')
	keyboard.press(Key.enter)
	keyboard.release(Key.enter)
	keyboard.press(Key.shift)
	keyboard.release(Key.shift)

for i in range(0, 100):
	# 获取进程id
	ps = psutil.pids()
	# 判断是否有putty进程，默认只有一个putty
	for pi in ps:
		p = psutil.Process(pi)
		if p.name().find("putty") == 0:
			autoinput()
			retun
	time.sleep(1)
