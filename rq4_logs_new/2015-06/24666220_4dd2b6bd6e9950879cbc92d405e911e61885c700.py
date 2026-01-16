from PySide import QtCore, QtGui

class MakinButton(QtGui.QPushButton):
	"""custom qpushbutton ta kei ono sinyal dihover & dileave"""
	"""gawe slot? http://www.pythoncentral.io/pysidepyqt-tutorial-creating-your-own-signals-and-slots/"""
	try:#--for pyqt4
		dihover = QtCore.pyqtSignal()
		dileave = QtCore.pyqtSignal()
	except:#-- for pyside 
		dihover = QtCore.Signal()
		dileave = QtCore.Signal()
		
	def __init__(self,text="",parent=None):
		super(MakinButton,self).__init__(text,parent)
		
	def enterEvent(self, event):
		self.dihover.emit()
		return QtGui.QPushButton.enterEvent(self, event)
		
	def enterEvent(self, event):
		self.dileave.emit()
		return QtGui.QPushButton.leaveEvent(self, event)
	
class MakinButtonExit(MakinButton):
	"""njajal Custom qpushbutton animasi hover & leave"""
	def __init__(self,text="",parent=None,vmin=0,vmax=200,vminr=3,vmaxr=40,rgb="50,50,50",rgbhover="10,10,10"):
		super(MakinButtonExit,self).__init__(text,parent)
		self.minValue = vmin #-- for alpha, from zero
		self.maxValue = vmax
		self.minValueR = vminr #-- for border radius
		self.maxValueR = vmaxr
		self.alphav = self.minValue
		self.border = self.maxValueR
		self.textcolor = "black"
		self.setStyleSheet("background-color:rgba("+rgb+","+str(self.alphav)+");color:"+self.textcolor+";")
		self.rgbhover = rgbhover
		self.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
		self.animTimerIn = QtCore.QTimer(self)
		self.animTimerIn.timeout.connect(self.colorIn)
		self.animTimerOut = QtCore.QTimer(self)
		self.animTimerOut.timeout.connect(self.colorOut)
	
	def colorIn(self):
		"""animasi perbarui warna alpha """
		self.alphav += 1
		self.border += 1
		if (self.alphav>=self.maxValue):
			self.animTimerIn.stop()
		self.animate(self.alphav,((self.border-self.minValue)*(self.maxValueR-self.minValueR)/(self.maxValue-self.minValue)	))
		#~ self.textcolor = "white"
			
	def colorOut(self):
		"""animasi perbarui warna alpha """
		self.alphav -= 1
		self.border -= 1
		if (self.alphav<=self.minValue):
			self.animTimerOut.stop()
		if (self.alphav>=self.minValue):
			#~ self.animate(self.alphav,(self.border/20))
			self.animate(self.alphav,((self.border-self.minValue)*(self.maxValueR-self.minValueR)/(self.maxValue-self.minValue)	))
		#~ self.textcolor = "black"
	
	def enterEvent(self, event):
		"""reimplement enterEvent bro"""
		self.alphav = self.minValue
		self.border = self.minValue #-- samakan, diskala di animate calling
		self.animTimerIn.start(2)
		
		return QtGui.QPushButton.enterEvent(self, event)
		
	def leaveEvent(self, event):
		self.alphav = self.maxValue
		self.border = self.maxValue #-- samakan, diskala di animate calling
		self.animTimerOut.start(2)
		
		return QtGui.QPushButton.leaveEvent(self, event)
	
	def animate(self,alphav,radius=0):
		warna = "rgba("+self.rgbhover+","+str(alphav/4)+")"
		self.setStyleSheet(	"background-color:"+warna+"; border-style:outset; border-color:"+warna+"; border-width:1px;border-radius:"+str(radius)+"px;color:"+self.textcolor+";")
		
	