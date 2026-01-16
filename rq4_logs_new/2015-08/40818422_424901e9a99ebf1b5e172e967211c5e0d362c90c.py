import grovepi
from math import log 
from thermocouples_reference import thermocouples

class HighTemp():
	_DEBUG = False
	_VOL_OFFSET = 337
	_AMP_AV = 54.16 
	def __init__(self):
		self._ThmcPin = None
		self._NtcPin = None
		self.typeK = thermocouples['K']
	def setDebug(self):
		self._DEBUG = True
	def setThmcPin(self,value):
		self._ThmcPin = value
		grovepi.pinMode(self._ThmcPin,"INPUT")
	def setNtcPin(self,value):
		self._NtcPin = value
		grovepi.pinMode(self._NtcPin,"INPUT")
	def _readPin(self,pinValue):
		voltage = None
		errorCpt = 0
		while( (errorCpt<10) and (voltage is None) ):
			try:
				voltage = grovepi.analogRead(pinValue)
				if self._DEBUG:
					print "readPin, voltage =" + str(voltage)
			except IOError:
				errorCpt+=1
				if self._DEBUG:
					print "readPin, errorCpt =" + str(errorCpt)
		return voltage
	def getNtcTemp(self):
		ntcValue = self._readPin(self._NtcPin)
		if ntcValue == 0:
			return None 
		else:
			a = ntcValue*50/33
			if self._DEBUG:
				print "NTC, a=" + str(a)
			resistance=(1023.0-a)*10000.0/a
			if self._DEBUG:
				print "NTC, resistance =" + str(resistance)
			temperature=1/(log(resistance/10000.0)/3975+1/298.15)-273.15
			return temperature
	def _getThmcMv(self):
		thmcValue = self._readPin(self._ThmcPin)
		vout = thmcValue/1023.0*5000
		if self._DEBUG:
			print "Mv, vout =" + str(vout)
		vin = (vout - self._VOL_OFFSET)/self._AMP_AV
		if self._DEBUG:
			print "Mv, vin =" + str(vin)
		return vin
	def getThmcTemp(self):				
		return self.typeK.inverse_CmV(self._getThmcMv(), Tref=self.getNtcTemp())
