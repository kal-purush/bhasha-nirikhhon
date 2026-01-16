# python3
import sys
import os
substring = input ("enter your string (Ex:20180810): ")
filename = input ("enter your POD file name (xxx.txt): ")
count =0		
#for line in open('POD_4518.txt'):
for line in open(filename):
    #if ("") in line:
    if substring in line:
       line = line.strip()
	
       count +=1
       print (line)
if count==0:
    print ("string not found")
os.system("pause")  
    