import sys
def helpMsg():
	print ('-h  visualizza questo messaggio\n-c  conta e controlla le colonne di un file <nomefile> <*separatore>')
	print ('\n*aggiungere ^ se viene utilizzato un escape character')
def contatore (file,lineSeparator):
	s=0
	flag =True
	lineCounter=0
	for line in txt:
		col= line.split(line_separator)
		lineCounter+=1
		for c in col:
			s+=1
		if (flag==True):
			ss=s
			flag=False
		if (ss!=s):
			print ("Trovato numero colonne diverso, riga <",lineCounter,">")
		s=0
	#break #togliere per controllare tutto il file
	print ("Il numero di colonne e' uniforme, colonne <",ss,">")	

inizio="-_-_-_-_-_-_-TRAFITTO-_-_-_-_-_-_-\n\n\tTXT-CSV_Controller\n"
print (inizio)
try:
	command=sys.argv[1]
	
except :
	helpMsg()
	sys.exit()

if (command=='-h'):
	helpMsg()
elif (command=='-c'):
	try:	
		file_name=sys.argv[2]
		line_separator=sys.argv[3]
	except:
		helpMsg()	
	try:
		txt=open(file_name,'r')
		contatore(txt,line_separator)
	except:
		print ("File non trovato\n")
		helpMsg()
	
	

	