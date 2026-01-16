# the main UI
# check : Gtk-Message: 20:05:27.575: GtkDialog mapped without a transient parent. This is discouraged. 

''''
variable declaration:-
1) self.main_flag:- 	status -class string
						it is used to notify the process is encryption or decryption
2) self.key:-	status - class member variable string
				It contains the 16 digit key for both enc and dec 					 
3) self.in_path:-	status- class member string var
					string input file adr with file name		
4) self.op_path:- 	status- class member string var
					op path only , file name not included
					
5)self.log_path:-	status- class member string var
					log file path
					
6)process_flag					
''' 

'''
FUNCTIONs:-
'''

from PyQt5.QtWidgets import *
from PyQt5.QtGui import *

from Crypto.Cipher import AES
from Crypto import Random
from random import randint,seed,shuffle

from time import ctime,sleep
import sys
import os
import struct
import csv


padd=['#','@','!','$','%','&','?','>','<','+','~','*']

class App(QWidget):
	"""   """
	def selector(self,text):
		if text=='ENCRYPTION':
			self.main_flag='e'
		elif text=='DECRYPTION' :
			self.main_flag='d'
		print(self.main_flag)	
	
	def check(self):
		print("hello")
			
	
	def onActivated(self,text):
		if text=='AES':
			self.myMessageBox.setText("AES is selected, MODE:-CBC , Now enter the key ")
			self.process_flag='AES'
			print(self.in_path)
			if self.main_flag=='e':
				l=self.in_path.split('/')
				fname=l[-1]
				print(fname)
				self.op_path=self.op_path+'/'+fname+'.enc'
			elif self.main_flag=='d':
				self.op_path=os.path.splitext(self.in_path)[0]					
				
	
	def key_generator(self,text):
		if len(text)==8 and self.main_flag=='e':  ## this part is for encryption
			self.key=text;
			key_length=len(self.key)
			if	(key_length>12): 
				self.key=self.key[:12] # if input key length over 12 truncate it
				key_length=12

			#print(key)
			padding_length=16-key_length;
			i=0
			## randomness generator  seed()
			seed()
			for i in range(padding_length):
				l=len(padd)
				x=randint(0,l-1)
				self.key=self.key+padd[x]

			#print(key)  # now the 16 byte key is ready
			'''
			# shufflling
			self.key=list(self.key)
			shuffle(self.key)
			self.key=''.join(self.key)
			## shuffled key is ready
			''' 
			print(self.key)
			
		if self.main_flag=='d'	and len(text)==16:   ## this part is for decryption
			self.key=text
			print(self.key)
				
		
	def open_files(self):   # browser Button
		dialog = QFileDialog()
		fname = dialog.getOpenFileName(self, "Open file")
		filename=fname[0]
		self.in_path=filename
		self.myTextBox.setText(self.in_path)
		
	def open_files_log(self):   # log file browser Button
		dialog = QFileDialog()
		fname = dialog.getOpenFileName(self, "Open log file")
		filename=fname[0]
		self.log_path=filename
		self.logTextBox.setText(filename)
	
	def startP(self):
		print(self.process_flag)
		if self.process_flag=='AES':
			if len(self.key)!=16:
				#print(self.key)
				#len(self.key)
				self.myMessageBox.setText("You have to enter key")
			elif len(self.key)==16 and self.main_flag=='e':
				#	now let's  do the encryption
				self.myMessageBox.setText("Encryption is going on")
								'''
				missing
				'''
				
				self.myMessageBox.setText("Encryption completed")
			
			elif len(self.key)==16 and self.main_flag=='d':
				""" decryption """
				self.myMessageBox.setText("Decryption is going on")			
				'''
				missing
				'''
				self.myMessageBox.setText("Decryption is completed")				
				
		
			
	def __init__(self):
		super().__init__()
		self.initUI()

	def initUI(self):
		self.main_flag=''
		self.op_path='/home/pabitra/Encrypto'  ## folder not file	
		self.key=''
		#self.in_path=''	
		
		self.log_path='/home/pabitra/Encrypto/crypto_log.csv'
		f=open(self.log_path,'a')
		f.close()
		
		self.process_flag=''
		self.chunksize=64*1024
			
		# Basic tile,icon,geometry,statusBar
		self.setWindowTitle("Encrypto")
		self.setWindowIcon(QIcon("14970129516348746919"))
		self.setGeometry(100,100,420,280)		
		
		# create widgets
		
		## 1.1) browse button to select file to be encrypted or decrypted		
		browserButton=QPushButton("browse files ",self)
		browserButton.resize(browserButton.sizeHint())
		browserButton.setToolTip("Press to select the file you want ")
		browserButton.clicked.connect(self.open_files)
		#print(self.result)
		## 1.2) this is a text box to show the file selected
		self.myTextBox=QTextEdit(self)   
		
		## 2) combobox to select type of encryption
		self.bil_type=QLabel("<b>Select the type of encryption<\b>")
		combo = QComboBox(self)
		combo.addItem("Select")
		combo.addItem('AES')
		combo.addItem('DES3')
		combo.addItem('XOR')
		combo.activated[str].connect(self.onActivated)
		
		## 3) key enter
		self.bil_key=QLabel("<b>Key <\b>")
		self.keyBox=QLineEdit(self)
		try:
			self.keyBox.textChanged[str].connect(self.key_generator)
		except:
			pass
				
		
		## 4)log file 
		self.bil_log=QLabel("<b>log<\b>")
		self.log_browser=QPushButton("select log file  ",self)
		self.log_browser.resize(browserButton.sizeHint())
		self.log_browser.setToolTip("Press to select the file you want ")
		self.log_browser.clicked.connect(self.open_files_log)
		self.logTextBox=QTextEdit(self)
		self.logTextBox.setText("/home/pabitra/Encrypto/crypto_log.csv")
		
		## 5) select encryption or decryption
		self.bil_main=QLabel("<b>Choice  <\b>")
		self.select = QComboBox(self)
		self.select.addItem(' select ')
		self.select.addItem('ENCRYPTION')
		self.select.addItem('DECRYPTION')
		self.select.activated[str].connect(self.selector)
		
		
		## 6) start the process		
		startButton= QPushButton("START",self)
		startButton.resize(startButton.sizeHint())
		startButton.setToolTip("Press to  run the Encryption ")
		startButton.clicked.connect(self.startP)
		
		## 7) message box		
		self.bil_msg=QLabel("<b>Message Box<\b>")	
		self.myMessageBox=QTextEdit(self)
		self.myMessageBox.setText("Please enter all the credential for the program to run")
		
		
		## grid
		grid=QGridLayout()
		grid.setSpacing(10)
		grid.addWidget(self.bil_main,1,0)
		grid.addWidget(self.select,2,0)
		grid.addWidget(browserButton,3,0) ## browse file
		grid.addWidget(self.myTextBox,3,1)
		grid.addWidget(self.bil_log,4,0)
		grid.addWidget(self.log_browser,5,0)
		grid.addWidget(self.logTextBox,5,1) ##
		grid.addWidget(self.bil_type,6,0)
		grid.addWidget(combo,7,0)
		grid.addWidget(self.bil_key,8,0)
		grid.addWidget(self.keyBox,9,0)
		grid.addWidget(self.bil_msg,10,0)
		grid.addWidget(self.myMessageBox,11,0)
		grid.addWidget(startButton,12,0)
		self.setLayout(grid)
		
		self.show()
	
	def closeEvent(self,event):
		reply=QMessageBox.question(self,"Message","Quit ?",QMessageBox.Yes | QMessageBox.No,QMessageBox.No)		
		if reply==QMessageBox.Yes:
			event.accept()
		if reply==QMessageBox.No :
			event.ignore()
					
	
if __name__ == '__main__':
	app=QApplication(sys.argv)
	r=App()
	sys.exit(app.exec_())
# the main UI
# check : Gtk-Message: 20:05:27.575: GtkDialog mapped without a transient parent. This is discouraged. 

''''
variable declaration:-
1) self.main_flag:- 	status -class string
						it is used to notify the process is encryption or decryption
2) self.key:-	status - class member variable string
				It contains the 16 digit key for both enc and dec 					 
3) self.in_path:-	status- class member string var
					string input file adr with file name		
4) self.op_path:- 	status- class member string var
					op path only , file name not included
					
5)self.log_path:-	status- class member string var
					log file path
					
6)process_flag					
''' 

'''
FUNCTIONs:-
'''

from PyQt5.QtWidgets import *
from PyQt5.QtGui import *

from Crypto.Cipher import AES
from Crypto import Random
from random import randint,seed,shuffle

from time import ctime,sleep
import sys
import os
import struct
import csv


padd=['#','@','!','$','%','&','?','>','<','+','~','*']

class App(QWidget):
	"""   """
	def process_func(self,text):
		if text=='Encrytion':
			self.main_flag='e'
		elif text=='Decryption' :
			self.main_flag='d'
		print(self.main_flag)	
	
	def check(self):
		print("hello")
			
	
	def method_func(self,text):
		if text=='Blowfish':
			###self.myMessageBox.setText("AES is selected, MODE:-CBC , Now enter the key ")
			self.method_flag='Blowfish'
			print(self.in_path)
			'''if self.main_flag=='e':
				l=self.in_path.split('/')
				fname=l[-1]
				print(fname)
				self.op_path=self.op_path+'/'+fname+'.enc'
			elif self.main_flag=='d':
				self.op_path=os.path.splitext(self.in_path)[0]	'''				
				
	
	def key_generator(self,text):
		pass
			
		
				
		
	def open_files(self):   # browser Button
		dialog = QFileDialog()
		fname = dialog.getOpenFileName(self, "Open file")
		filename=fname[0]
		self.in_path=filename
		self.myMessageBox.setText(self.in_path)
		
	def open_files_log(self):   # log file browser Button
		dialog = QFileDialog()
		fname = dialog.getOpenFileName(self, "Open log file")
		filename=fname[0]
		self.log_path=filename
		self.logTextBox.setText(filename)
	
	def startP(self):
		pass
		
			
	def __init__(self):
		super().__init__()
		self.initUI()

	def initUI(self):
		self.main_flag=''
		self.op_path='/home/pabitra/Encrypto'  ## folder not file	
		self.key=''
		#self.in_path=''	
		
		'''self.log_path='/home/pabitra/Encrypto/crypto_log.csv'
		f=open(self.log_path,'a')
		f.close()'''
		
		self.method_flag=''
		self.chunksize=64*1024
			
		# Basic tile,icon,geometry
		self.setWindowTitle("Crypt")
		self.setWindowIcon(QIcon("14970129516348746919"))
		self.setGeometry(100,100,420,280)		
		
		# create widgets
		# a
		self.label_wel=QLabel("<b>WELCOME TO YOUR PERSONAL CONVERTER<\b>")
		
		#b.a
		self.label_file=QLabel("Open File")
		#b.b
		self.myMessageBox=QTextEdit(self)
		#b.c
		browserButton=QPushButton("browse files ",self)
		browserButton.resize(browserButton.sizeHint())
		browserButton.setToolTip("Press to select the file you want ")
		#browserButton.clicked.connect(self.open_files)
		self.myMessageBox.setText("Please enter all the credential for the program to run")
		
		#c.a
		self.label_meth=QLabel("Method of Encrytion/Decryption")
		#c.b
		combo1 = QComboBox(self)
		combo1.addItem("Select")
		combo1.addItem('Blowfish')
		combo1.addItem('XOR')
		#combo1.activated[str].connect(self.method_func)
		#c.c
		self.label_pro=QLabel("Process")
		combo2 = QComboBox(self)
		combo2.addItem("Select")
		combo2.addItem('Encryption')
		combo2.addItem('Decryption')
		#combo2.activated[str].connect(self.process_func)
		
		#d.a
		self.label_key=QLabel("Key Setting")
		self.keyBox=QLineEdit(self)
		try:
			self.keyBox.textChanged[str].connect(self.key_generator)
		except:
			pass
			
		#e
		subButton= QPushButton("SUBMIT",self)
		subButton.resize(subButton.sizeHint())
		subButton.setToolTip("Press to  run the Encryption ")
		subButton.clicked.connect(self.startP)
		
		#f.a
		self.label_mb=QLabel("Message Box")
		#f.b
		self.myMessage=QTextEdit(self)
		self.myMessage.setText("Please enter all the credential for the program to run")
		
		
		## grid
		grid=QGridLayout()
		grid.setSpacing(10)
		grid.addWidget(self.label_wel,1,0)
		grid.addWidget(self.label_file,2,0)
		grid.addWidget(self.myMessageBox,3,0) 
		grid.addWidget(browserButton,3,1)
		grid.addWidget(self.label_meth,4,0)
		grid.addWidget(self.label_pro,4,1)
		grid.addWidget(combo1,5,0)
		grid.addWidget(combo2,5,1)
		grid.addWidget(self.label_key,6,0)
		grid.addWidget(self.keyBox,7,0)
		grid.addWidget(subButton,8,0)
		grid.addWidget(self.label_mb,9,0)
		grid.addWidget(self.myMessage,10,0)
		self.setLayout(grid)
		
		self.show()
	
	def closeEvent(self,event):
		reply=QMessageBox.question(self,"Message","Quit ?",QMessageBox.Yes | QMessageBox.No,QMessageBox.No)		
		if reply==QMessageBox.Yes:
			event.accept()
		if reply==QMessageBox.No :
			event.ignore()
					
	
if __name__ == '__main__':
	app=QApplication(sys.argv)
	r=App()
	sys.exit(app.exec_())