from random import shuffle
import numpy

matrix=numpy.chararray((7,10),itemsize=10)
matrix[:]='None'
days=['Mon','Tue','Wed','Thurs','Fri','Sat']
period=[]
for i in range(0,9):
    period.append(`i+1`)
Lname=[]
name="Name"

def Class():
    j=1
    for i in days:
        matrix[[j],[0]]=numpy.array([i])
        j+=1
    j=0
    for i in period:
        matrix[[0],[j]]=numpy.array([i])
        j+=1
    for i in range(0,6):
        Lname.append((name+`i+1`))
    Lname.append('LAB')
    shuffle(Lname)
    count=0
    for l in range(1,len(days)+1):    
            k=1
            for i in Lname:
                shuffle(Lname)
                matrix[[l],[k]]=numpy.array([i])
                k+=1
    print matrix

Class()

