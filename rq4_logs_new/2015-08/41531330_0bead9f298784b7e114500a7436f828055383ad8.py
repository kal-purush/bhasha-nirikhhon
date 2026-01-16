import serial

class GpsModule (object):
	def __init__ (self, port1 = '/dev/ttyUSB0', baudrate = 9600, timeout = 1):
        	self.baudrate = baudrate
		self.timeout = timeout
		self.port = port1
		
	def getGPS (self):
		port = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
		line = port.readline()
		while line[4]!='G': #it has to be $GPGGA
			line = port.readline()	
		if line[4]=='G':
			lati=line[18:27]
			longi=line[31:40]
		latitude=float(lati)
		longitude=float(longi)		
		return [latitude, longitude] #should probably add hemisphere info
