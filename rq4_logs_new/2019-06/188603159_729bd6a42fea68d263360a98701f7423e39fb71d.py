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
					
6)self.process_flag:- 	status- class member string variable
						keep track of the methode used  					
''' 
#=============================================================================================================#
'''
FUNCTIONs:-

1)

'''


#from fbs_runtime.application_context import ApplicationContext
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *

from Crypto.Cipher import AES,DES3
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
		#print(self.main_flag)	
	
	def check(self):
		print("hello")
			
	
	def onActivated(self,text):
		#print(self.in_path)
		if self.main_flag=='e':
			l=self.in_path.split('/')
			fname=l[-1]
			#print(fname)
			self.op_path=self.op_path+'/'+fname+'.enc'
		elif self.main_flag=='d':
			self.op_path=os.path.splitext(self.in_path)[0]
		if text=='AES':
			self.process_flag='AES'
			self.myMessageBox.setText("AES is selected, MODE:-CBC , Now enter the key ")
		elif text=='DES3':
			self.process_flag='DES3'
				
								
				
	
	def key_generator(self,text):
		if len(text)==8 and self.main_flag=='e' and (self.process_flag=='DES3' or self.process_flag=='AES'):  ## this part is for encryption
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
			## Uncomment the following if you want to do a shuffling in the key
			'''
			# shufflling
			self.key=list(self.key)
			shuffle(self.key)
			self.key=''.join(self.key)
			## shuffled key is ready
			''' 
			#print(self.key)
			self.myMessageBox.setText(self.key)
			self.key=self.key.encode('utf-8')
			
		if self.main_flag=='d'	and len(text)==16 and (self.process_flag=='DES3' or self.process_flag=='AES'):   
			## this part is for decryption
			self.key=text
			#print(self.key)
			self.myMessageBox.setText(self.key)
			self.key=self.key.encode('utf-8')
				
		
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
		
	def op_files(self):
		self.op_path= str(QFileDialog.getExistingDirectory(self, "Select Directory"))	
		self.opTextBox.setText(self.op_path)
		
	
	def startP(self):
		#print(self.process_flag)
		if self.process_flag=='AES':
			if len(self.key)!=16:
				#print(self.key)
				len(self.key)
				self.myMessageBox.setText("You have to enter key")
				
				### Encryption AES ====================>>>>>>>>>>>>
			elif len(self.key)==16 and self.main_flag=='e':
				#	now let's  do the encryption
				self.myMessageBox.setText("Encryption is going on")
				key=self.key
				in_filename=self.in_path
				log_path=self.log_path 
				out_filename=self.op_path
				chunksize=self.chunksize;
				#========================================================#
				log=[]
				curr_time=ctime()
				cTime=curr_time.split(' ')
				date=cTime[1]+' '+cTime[2]+' '+cTime[-1]
				day=cTime[0]
				clk=cTime[3]
	
				log.append(day)
				log.append(date)
				log.append(clk)
				
				temp_key=key.decode("utf-8")
				log.append(temp_key)
				log.append(in_filename)
		
				if  out_filename=='':
					out_filename = in_filename + '.enc'
				
				# system key file	
				iv =Random.new().read(AES.block_size)
				encryptor = AES.new(key, AES.MODE_CBC, iv)
				filesize = os.path.getsize(in_filename)

				with open(in_filename,'rb') as infile:
					with open(out_filename,'wb') as outfile:
						outfile.write(struct.pack('<Q', filesize))
						outfile.write(iv)
						
						#####
						
						'''
						i=0
						wstr=''
						for i in range(16):
							wstr=wstr+' '

						print(len(wstr))
						wstr=wstr.encode("utf-8")
						outfile.write(encryptor.encrypt(wstr))
                                                '''

						while True:
							chunk = infile.read(chunksize)
							#print(type(chunk))
							if len(chunk) == 0:
								break
							elif len(chunk) % 16 != 0:
								chunk += b' ' * (16 - len(chunk) % 16)

							outfile.write(encryptor.encrypt(chunk))
				outfile.close()
				infile.close()
				log.append(out_filename)
				log.append(self.process_flag)
				#logging
				with open(log_path,'a') as logf:
					writer = csv.writer(logf)
					writer.writerow(log)
					logf.close()
				##sleep(0.8)
				self.myMessageBox.setText("Encryption completed ... please close the window")
				sleep(1)
				self.close()
			
			# DECRYPTION ============>>>>>>>>>>>>>>>>>>
			elif len(self.key)==16 and self.main_flag=='d':
				""" decryption """
				#self.key=self.key.decode('utf-8')
				self.myMessageBox.setText("Decryption is going on")
				key=self.key
				in_filename=self.in_path
				log_path=self.log_path
				out_filename=self.op_path 
				chunksize=self.chunksize;
				#=========================================#
				log=[]
				curr_time=ctime()
				cTime=curr_time.split(' ')
				date=cTime[1]+' '+cTime[2]+' '+cTime[-1]
				day=cTime[0]
				clk=cTime[3]
	
				log.append(day)
				log.append(date)
				log.append(clk)
	
				temp_key=key.decode("utf-8")
				log.append(temp_key)
				log.append(in_filename)
	
				if not out_filename:
					out_filename = os.path.splitext(in_filename)[0]

				with open(in_filename, 'rb') as infile:
					original_size = struct.unpack('<Q', infile.read(struct.calcsize('Q')))[0]
					iv = infile.read(16)
					decryptor = AES.new(key, AES.MODE_CBC, iv)

					with open(out_filename, 'wb') as outfile:
						while True:
							chunk = infile.read(chunksize)
							if len(chunk) == 0:
								break
							outfile.write(decryptor.decrypt(chunk))

						outfile.truncate(original_size)
						outfile.close() ###  Change ??
				log.append(out_filename)
				log.append(self.process_flag)
				
				#logging
				with open(log_path,'a') as logf:
					writer = csv.writer(logf)
					writer.writerow(log)
				logf.close()	
				
				self.myMessageBox.setText("Decryption completed ... please close the window")
				sleep(1)
				self.close()  
				
		##########################################################################################################################################	##########################################################################################################################################
				
		if	self.process_flag=='DES3':
			if len(self.key)!=16:
				#print(self.key)
				len(self.key)
				self.myMessageBox.setText("You have to enter key")
				
			elif len(self.key)==16 and self.main_flag=='e':
				#now let's  do the ENCRYPTION ==============>>>>>>>>>>>>>>>>
				self.myMessageBox.setText("Encryption is going on")
				key=self.key
				in_filename=self.in_path
				log_path=self.log_path 
				out_filename=self.op_path
				chunksize=self.chunksize;
				# **** #
				log=[]
				curr_time=ctime()
				cTime=curr_time.split(' ')
				date=cTime[1]+' '+cTime[2]+' '+cTime[-1]
				day=cTime[0]
				clk=cTime[3]
	
				log.append(day)
				log.append(date)
				log.append(clk)
	
				temp_key=key.decode("utf-8")
				log.append(temp_key)
				log.append(in_filename)
		
				if  out_filename=='':
					out_filename = in_filename + '.enc'
				
				# system key file	
				iv =Random.new().read(DES3.block_size)
				encryptor = DES3.new(key, DES3.MODE_OFB, iv)
				filesize = os.path.getsize(in_filename)

				with open(in_filename, 'rb') as infile:
					with open(out_filename, 'wb') as outfile:
						outfile.write(struct.pack('<Q', filesize))
						outfile.write(iv)

						while True:
							chunk = infile.read(chunksize)
							#print(type(chunk))
							if len(chunk) == 0:
								break
							elif len(chunk) % 8 != 0:
								chunk += b' ' * (8 - len(chunk) % 8)

							outfile.write(encryptor.encrypt(chunk))
				outfile.close()
				log.append(out_filename)
				log.append(self.process_flag)
				#logging
				with open(log_path,'a') as logf:
					writer = csv.writer(logf)
					writer.writerow(log)
					logf.close()
				self.myMessageBox.setText("Encryption completed ... please close the window")
				sleep(1)
				self.close()
				
			elif len(self.key)==16 and self.main_flag=='d':
				#""" Decryption ================>>>>>>>>> """
				self.myMessageBox.setText("Decryption is going on")
				key=self.key
				in_filename=self.in_path
				log_path=self.log_path
				out_filename=self.op_path 
				chunksize=self.chunksize;
				
				log=[]
				curr_time=ctime()
				cTime=curr_time.split(' ')
				date=cTime[1]+' '+cTime[2]+' '+cTime[-1]
				day=cTime[0]
				clk=cTime[3]
	
				log.append(day)
				log.append(date)
				log.append(clk)
	
				temp_key=key.decode("utf-8")
				log.append(temp_key)
				log.append(in_filename)
	
				if out_filename=='':
					out_filename = os.path.splitext(in_filename)[0]

				with open(in_filename, 'rb') as infile:
					original_size = struct.unpack('<Q', infile.read(struct.calcsize('Q')))[0]
					iv = infile.read(8)
					decryptor = DES3.new(key, DES3.MODE_OFB, iv)

					with open(out_filename, 'wb') as outfile:
						while True:
							chunk = infile.read(chunksize)
							if len(chunk) == 0:
								break
							outfile.write(decryptor.decrypt(chunk))

						outfile.truncate(original_size)
						outfile.close()
				log.append(out_filename)
				log.append(self.process_flag)
				#logging
				with open(log_path,'a') as logf:
					writer = csv.writer(logf)
					writer.writerow(log)
					logf.close()
				self.myMessageBox.setText("Decryption completed ... please close the window")
				sleep(1)
				self.close()				
											
				
			
	def __init__(self):
		super().__init__()
		self.initUI()

	def initUI(self):
		self.main_flag=''
		########################### EDIT TO CHANGE OUTPUT PATH
		self.op_path='' ## folder not file
		###########################################################	
		self.key=''
		#self.in_path=''	

		## log file creation
		self.log_path='log.csv' 
		f=open(self.log_path,'a')
		f.close()
		
		self.process_flag=''
		self.chunksize=64*1024
			
		# Basic tile,icon,geometry,statusBar
		self.setWindowTitle("Encrypto")
		self.setWindowIcon(QIcon("14970129516348746919"))
		self.setGeometry(100,100,420,280)		
		
		## 5) select encryption or decryption
		self.bil_main=QLabel("<b>Choice  <\b>")
		self.select = QComboBox(self)
		self.select.addItem(' select ')
		self.select.addItem('ENCRYPTION')
		self.select.addItem('DECRYPTION')
		self.select.activated[str].connect(self.selector)
		
		
		## 1.1) browse button to select file to be encrypted or decrypted
		self.bil1=QLabel("<b>Define input file <\b>")		
		browserButton=QPushButton("browse files ",self)
		browserButton.resize(browserButton.sizeHint())
		browserButton.setToolTip("Press to select the file you want ")
		browserButton.clicked.connect(self.open_files)
		#print(self.result)
		## 1.2) this is a text box to show the file selected
		self.myTextBox=QTextEdit(self)   
		
		
		## 4)log file 
		self.bil_log=QLabel("<b>log<\b>")
		self.log_browser=QPushButton("select log file  ",self)
		self.log_browser.resize(browserButton.sizeHint())
		self.log_browser.setToolTip("Press to select the file you want ")
		self.log_browser.clicked.connect(self.open_files_log)
		self.logTextBox=QTextEdit(self)
		self.logTextBox.setText("Defaultly Selected")
		
		## 8) op path
		self.bil_op=QLabel("<b>Define output path <\b>")		
		opButton=QPushButton("select the path ",self)
		opButton.resize(opButton.sizeHint())
		opButton.setToolTip("select the output folder ")
		opButton.clicked.connect(self.op_files)
		
		## 8.2) this is a text box to show the file selected
		self.opTextBox=QTextEdit(self)   
		
		
		
		## 2) combobox to select type of encryption
		self.bil_type=QLabel("<b>Select the type of encryption<\b>")
		combo = QComboBox(self)
		combo.addItem("Select")
		combo.addItem('AES')
		combo.addItem('DES3')
		#combo.addItem('XOR')
		combo.activated[str].connect(self.onActivated)
		
		## 3) key enter
		self.bil_key=QLabel("<b>Key <\b>")
		self.keyBox=QLineEdit(self)
		try:
			self.keyBox.textChanged[str].connect(self.key_generator)
		except:
			pass
		
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
		grid.addWidget(self.bil1,3,0)
		grid.addWidget(browserButton,4,0) ## browse file
		grid.addWidget(self.myTextBox,4,1)
		grid.addWidget(self.bil_log,5,0)
		grid.addWidget(self.log_browser,6,0)
		grid.addWidget(self.logTextBox,6,1) 
		grid.addWidget(self.bil_op,7,0)
		grid.addWidget(opButton,8,0)
		grid.addWidget(self.opTextBox,8,1)
		grid.addWidget(self.bil_type,9,0)
		grid.addWidget(combo,10,0)
		grid.addWidget(self.bil_key,11,0)
		grid.addWidget(self.keyBox,12,0)
		grid.addWidget(self.bil_msg,13,0)
		grid.addWidget(self.myMessageBox,14,0)
		grid.addWidget(startButton,15,0)
		self.setLayout(grid)
		
		self.show()
	
	def closeEvent(self,event):
		reply=QMessageBox.question(self,"Message","Close ?",QMessageBox.Yes | QMessageBox.No,QMessageBox.No)		
		if reply==QMessageBox.Yes:
			event.accept()
		if reply==QMessageBox.No :
			event.ignore()
					

# ['Breeze', 'Oxygen', 'QtCurve', 'Windows', 'Fusion']	
if __name__ == '__main__':
	app=QApplication(sys.argv)
	app.setStyle('Fusion')
	r=App()
	sys.exit(app.exec_())
