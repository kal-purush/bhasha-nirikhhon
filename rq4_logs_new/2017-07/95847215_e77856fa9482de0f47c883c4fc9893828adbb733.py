from sys import argv

import os
import random

script = argv
name = str(script[0])
cmd = 'start payload.txt'
foldername = str(random.randint(0,100)*10)
os.system(cmd)

try:
	os.mkdir('clone')
	os.system("copy payload.txt clone")
	os.system(r"copy " + name + " clone")
except:
	os.mkdir('clone' + foldername)
	os.system("copy payload.txt clone" + foldername)
	os.system(r"copy " + name + " clone" + foldername)