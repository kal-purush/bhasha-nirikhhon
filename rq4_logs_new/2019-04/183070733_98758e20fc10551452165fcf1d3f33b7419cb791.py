"""
This is a mock GPIO library that allows testing RPi.GPIO code on Windows and other systems.
Values here are NOT guaranteed to be consistent with RPi.GPIO, but constant/method naming should be.

Most methods are intended to be empty placeholders to allow the code to be ran, but not function.
Some limited state information may be included for debugging.
"""

class MockGPIO:
	#Constants
	UNDEFINED=-1

	OUT = 0
	IN = 1

	PUD_UP = 2
	PUD_DOWN = 4

	BOARD=8
	BCM=16

	HIGH=True
	LOW=False

	RISING=128
	FALLING=256

	#State information
	class simulation:
		pin_setup = [(-1,-1)] * 40
		pin_states= [-1] * 40
		pin_detections=[]
		pin_events=[]

		mode = -1

		@staticmethod
		def dispatch_event(pin,edge):
			for event in MockGPIO.simulation.pin_events:
				if event[0]==pin and (event[1] is not None):# check the registered event's pin and edge
					event[1](pin) # call the callback defined

		@staticmethod
		def detect_pin(pin,edge):
			if (pin,edge) in MockGPIO.simulation.pin_detections:
				MockGPIO.simulation.dispatch_event(pin,edge)

		@staticmethod
		def power_pin(pin):
			print("   Simulating power up on pin "+str(pin))
			MockGPIO.simulation.detect_pin(pin,MockGPIO.RISING)
			MockGPIO.simulation.pin_states[pin]=MockGPIO.HIGH

		@staticmethod
		def depower_pin(pin):
			print("   Simulating power down on pin "+str(pin))
			MockGPIO.simulation.pin_states[pin]=MockGPIO.LOW
			MockGPIO.simulation.detect_pin(pin,MockGPIO.FALLING)


	@staticmethod
	def setmode(layout):
		print("[WARNING] Mock-GPIO is being used - this is only intended off-pi development/testing.")
		MockGPIO.simulation.mode = layout

	@staticmethod
	def getmode():
		return MockGPIO.simulation.mode

	@staticmethod
	def setup(pin, inout, pull_up_down = UNDEFINED, initial=LOW):
		print("setup(pin=%d,inout=%s,pull_up_down=%s,initial=%s)" % (pin, MockGPIO.inout2str(inout),MockGPIO.pud2str(pull_up_down),MockGPIO.state2str(initial)))
		MockGPIO.simulation.pin_setup[pin] = (inout,pull_up_down)
		MockGPIO.simulation.pin_states[pin] = initial

	@staticmethod
	def input(pin):
		print("input(pin=%d)" % pin)
		return MockGPIO.simulation.pin_states[pin]

	@staticmethod
	def wait_for_edge(pin,edge,timeout=None):
		print("wait_for_edge(pin=%d,edge=%s)" % (pin, MockGPIO.edge2str(edge)))
		print(" Simulating pin changes needed for edge detection")
		if MockGPIO.input(pin): #high
			if edge is MockGPIO.RISING:
				MockGPIO.simulation.depower_pin(pin)
				MockGPIO.simulation.power_pin(pin)
			elif edge is MockGPIO.FALLING:
				MockGPIO.simulation.depower_pin(pin)
			else:
				print("  unsupported edge")
		else: #low
			if edge is MockGPIO.RISING:
				MockGPIO.simulation.power_pin(pin)
			elif edge is MockGPIO.FALLING:
				MockGPIO.simulation.power_pin(pin)
				MockGPIO.simulation.depower_pin(pin)
			else:
				print("  unsupported edge")

	@staticmethod
	def add_event_detect(pin,edge):
		print("add_event_detect(pin=%d,edge=%s)" % (pin, MockGPIO.edge2str(edge)))
		MockGPIO.simulation.pin_detections.append((pin,edge))

	@staticmethod
	def add_event_callback(pin,callback,bouncetime=None):
		print("add_event_callback(pin=%d,callback=...)" % pin)
		MockGPIO.simulation.pin_events.append((pin,callback))

	@staticmethod
	def remove_event_callback(pin):
		print("remove_event_callback(pin=%d,callback=...)" % pin)
		MockGPIO.simulation.pin_events = [event for event in MockGPIO.simulation.pin_events if event[0]!=pin] # remove all events for this pin by replacing with others



	#utility functions
	@staticmethod
	def edge2str(edge):
		if edge is MockGPIO.UNDEFINED: return "UNDEFINED"
		return {MockGPIO.RISING:"RISING", MockGPIO.FALLING:"FALLING"}.get(edge,"UNKNOWN")

	@staticmethod
	def state2str(state):
		if state is MockGPIO.UNDEFINED: return "UNDEFINED"
		return {MockGPIO.HIGH:"HIGH", MockGPIO.LOW:"LOW"}.get(state,"UNKNOWN")

	@staticmethod
	def inout2str(inout):
		if inout is MockGPIO.UNDEFINED: return "UNDEFINED"
		return {MockGPIO.IN:"IN", MockGPIO.OUT:"OUT"}.get(inout,"UNKNOWN")

	@staticmethod
	def pud2str(inout):
		if inout is MockGPIO.UNDEFINED: return "UNDEFINED"
		return {MockGPIO.PUD_DOWN:"PUD_DOWN", MockGPIO.PUD_UP:"PUD_UP"}.get(inout,"UNKNOWN")
