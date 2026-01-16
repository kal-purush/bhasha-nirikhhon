#!/usr//bin/python
import sys
file=open(sys.argv[1])
list=[]
for line in file:
    word=line.strip()
#    print word
    list.append(word)
#print list
#print len(list)
num=len(list)
#print type(num)
#print type(list[1])
total=0
for i in range(num):
    total=total+int(list[i])
#print total
print "Total Byte= ",total,"byte"
print "Kilo Byte= ",total/1024,"KB"
print "Mega Byte= ",total/(1024*1024),"MB"
print "Giga Byte= ",total/(1024*1024*1024),"GB"


