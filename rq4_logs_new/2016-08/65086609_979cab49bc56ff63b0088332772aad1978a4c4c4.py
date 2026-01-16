import ugfx
import pyb
import buttons

def displayImage(path):
	ugfx.display_image(76,28, path)

def displaySpinCounter(num):
	ugfx.area(0, 0, 320, 60, 0x0000)
	if (num==1): 
		ugfx.text(10, 10, "You spinned the Doge 1 time .. ", 0xFFFF)
	else:
		ugfx.text(10, 10, "You spinned the Doge "+str(num)+" times .. ", 0xFFFF)

ugfx.area(0, 0, 320, 240, 0x0000)
buttons.init()

spinCounter = 0

while not buttons.is_pressed("BTN_MENU"):
	displayImage('apps/rootzoll~dogespin/1.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/2.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/3.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/4.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/5.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/6.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/7.gif')
	pyb.delay(300)
	displayImage('apps/rootzoll~dogespin/8.gif')
	pyb.delay(300)
	spinCounter = spinCounter + 1
	displaySpinCounter(spinCounter)