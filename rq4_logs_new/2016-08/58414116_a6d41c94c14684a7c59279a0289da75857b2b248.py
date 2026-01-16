import time

def start_timer():
	timer_start = int(round(time.time() * 1000))
	return timer_start

def end_timer(timer_start):
	print ((int(round(time.time() * 1000))) - timer_start)