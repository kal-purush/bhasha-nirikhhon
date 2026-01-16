
"""
width = 20
height = 5*9
answer = width * height
print(answer)
"""

"""
f = open(r'C:\Users\kaidi\Documents\GitHub\MyPythonTest\firststep.txt','w')
f.write('Hello! How are u?')
f.close()
f = open(r'C:\Users\kaidi\Documents\GitHub\MyPythonTest\firststep.txt','r')
F1 = f.read(5)
F2 = f.read()#Read the reat of the txt
f.close()
print F2
"""

"""
f = open(r'C:\Users\kaidi\Documents\GitHub\MyPythonTest\src.txt','w')
f.write('How many seas must a white dove sail')
f.write('\n')#new line
f.write('Before she sleeps in the sand')
f.close()
"""

"""

"""

import re
import math

f = open('main_building.gml')
f1 = open(r'dest.txt','w')

flag = 0
line = f.readline()
while line:
    if flag == 0:
        line = f.readline()
        if line.__contains__('edges'):
            flag = 1
    elif flag == 1:
        line = f.readline()
        f1.write(line)

f.close()
f1.close()


f = open('dest.txt')
f1 = open(r'Output.txt','w')
flag = 0
flag2 = 0
line = f.readline()
while line:
    if line.__contains__('start'):
        p1 = ''
        for i in line:
            if i == 'S':
                flag = 1
            if i == '"':
                flag = 0
            if flag == 1:
                p1 += i
        f1.write(p1+',')
        print 'p1='+p1
    if line.__contains__('end'):
        p2 = ''
        for i in line:
            if i == 'S':
                flag = 1
            if i == '"':
                flag = 0
            if flag == 1:
                p2 += i
        f1.write(p2+',')
        print 'p2='+p2
    if line.__contains__('gml:pos'):
        if flag2 == 0:
            linesplit = re.split(r'[>\s]\s*',line)
            x1 = linesplit[2]
            y1 = linesplit[3]
            flag2 = 1
            print 'x1='+x1
            print 'y1='+y1
        elif flag2 == 1:
            linesplit = re.split(r'[>\s]\s*',line)
            x2 = linesplit[2]
            y2 = linesplit[3]
            flag2 = 0
            dist = math.sqrt(math.pow(float(x1)-float(x2), 2)+math.pow(float(y2)-float(y1), 2))
            Dist = str(dist);
            f1.write(Dist[:10]+'\n')
            print 'x2='+x2
            print 'y2='+y2
            print 'dist='+Dist[:5]
    line = f.readline()
f.close()
f1.close()

"""
ST1:{ST4:3,ST5:1},
ST2:{a:2,c:1},
ST3:{a:4,b:1}
"""
f = open('Output.txt')
f1 = open(r'Final.txt','a')

flag = 0
line = f.readline()
"""
while line:
    data=line.split(",")
    f1.write(data[0]+":{"+data[1]+":"+data[2])
    print data[0]
    line=f.readline()
"""
# Here to set the range from start ST1 to end ST190
for i in range(1,191):
    first = "ST"+str(i)
    f = open('Output.txt')
    line = f.readline()
    f1.write(str(first)+":{")
    print first
    while line:
        data=line.split(",")

        print "Everyline"+data[0]
        if data[0]==first:
            print data[0]+'!!!!!'+first
            f1.write(data[1]+":"+data[2]+",")
        elif data[1]==first:
            f1.write(data[0]+":"+data[2]+",")



        line=f.readline()
    f1.write("},")
    f1.write('\n')
    f.close()

f.close()
f1.close()