#Author: Farmehr Farhour f.farhour@gmail
#Note: Python version: 2. Created in Windows (because of Tableau)

import json
#import os to open json files
import os



#append text file, line 1 + line 2. 
#input args: file=file name ; l1=text for line 1 (name of file); l2 = text for l2 (mteps)
def appendFile(file,l1,l2):
	if(os.path.isfile(file)):
		with open(file,'r') as f:
			#read a list of lines into data
			data = f.read().splitlines()
	else:
		data = ["",""]
	#append lines
	data[0] = data[0]+"'"+str(l1)+"' "+'\n'
	data[1] = data[1]+str(l2)+" "+'\n'
		
	#write everything back
	with open(file,'w') as f:
		f.writelines(data)
		
	
#main script here
newpath = r'../gunrock-output/txt_output' 
if not os.path.exists(newpath):
    os.makedirs(newpath)
for file in os.listdir('../gunrock-output'):
    if file.endswith(".json"):
		print file
		with open('../gunrock-output/'+file) as data_file:    
			json_parsed = json.load(data_file)
			print json_parsed['m_teps']
			appendFile("output.txt",file,json_parsed['m_teps'])
			#f = open('output.txt','a')
			#print f 
			#f.write(file+'\n'+str(json_parsed['m_teps']))
			#f.close()
		#json_parsed = json.loads(file)
		
		