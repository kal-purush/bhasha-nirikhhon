import threading
import time
i=0

def printValue(value):
	global i
	print value
	i+=1

class Producer(threading.Thread):
	global i
	def __init__(self,event):
		
		
		threading.Thread.__init__(self)
		self.event = event
	
	def run(self):
		
		while True:
			print '__In thread %s__' % self.name
			self.event.clear()          
			#---------------------------#
			
			print 'event clear' 		# Code for lock state starts
			print 'waiting for 3 sec'
			printValue(i)
			
			time.sleep(3)                 
			print 'after 3 sec'			#code for lock state ends
			
			#---------------------------# 
			self.event.set()			
			
			print 'event set'			#Other thread starts
			print 'waiting for 5 sec'
			time.sleep(5)
			print 'after 5 sec' 		#Other thread stops
			#---------------------------#

class Consumer(threading.Thread):
	global i
	
	def __init__(self,event):
		
		
		threading.Thread.__init__(self)
		self.event = event
	
	def run(self):
		
		while True:
			self.event.wait()
			printValue('\t\t\t value=' + str(i))
			
			time.sleep(1)
			
			

def main():
	event = threading.Event()
	t1 = Producer(event)
	t2 = Consumer(event)
	t1.start()
	t2.start()
	t1.join()
	t2.join()

if __name__ == '__main__':
	main()