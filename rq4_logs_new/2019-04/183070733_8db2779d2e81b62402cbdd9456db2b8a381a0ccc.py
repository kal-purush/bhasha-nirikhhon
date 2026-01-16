import RPi.GPIO as GPIO
import time
import subprocess

class BatterySens:

	def __init__(self, pin):
		self.called = False
		self.running = True
		self.pin = pin
		GPIO.setmode(GPIO.BOARD) #GPIO index, not pin-number, use GPIO.BOARD for pin-number
		GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
	def start(self):
		print("adding pin callback to pin "+str(self.pin))
		GPIO.add_event_detect(self.pin, GPIO.FALLING)
		GPIO.add_event_callback(self.pin, self.pin_callback)
	def pin_callback(self,value):
		print("on battery power! "+str(value))
		self.running=False




print("Starting BatterySens")
powersafety = BatterySens(11)
powersafety.start()

print("waiting...")
while powersafety.running:
	value = GPIO.input(11)
	print(" value="+str(value))
	time.sleep(1)

print("performing shutdown operations")
print("stopping mysql...")
subprocess.check_output("service mysql stop",shell=True)
print("stopped mysql.")
print("flushing disk caches...")
subprocess.check_output("sync",shell=True)
print("flushed.")
print("shutting down...")
subprocess.check_output("shutdown now",shell=True)