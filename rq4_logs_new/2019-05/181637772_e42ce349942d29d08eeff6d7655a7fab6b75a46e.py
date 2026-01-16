import os
from array import array

def connectivity(dirsti, fil):
    A=[]
    with open('%s/%s'%(dirsti,fil),'r') as f:
        lines = f.readlines()
        for line in lines:
            linje = line.rstrip()
            A.append(linje.split(","))
    print (A)
    nodetjek = []
    for entry in range(len(A)):
        nodetjek.append(1)
    print(nodetjek)
    
    connection = A[0]
    print(connection)
    tjekker = True
    while tjekker == True:
        print(tjekker)
        tjekker=False

        for entry in range(len(connection)):
            print("entry%s"%entry)
            print(nodetjek[int(entry)])
            if nodetjek[int(entry)] == 1 and connection[entry] != 0:
                tjekker=True
                print(len(A))
                print(len(connection))
                row = []
                row = A[int(entry)]
                print("c1%s"%connection)
                print("A%s"%A[int(entry)])
                for x in range(len(connection)):
                    value = A[int(entry)]
                    connection[x] = int(connection[x]) + int(value[x])
                print("C2%s"%connection)
                print(nodetjek)
                nodetjek[int(entry)] = 0
                break
    for entry in range(len(connection)):
        if connection[entry]!=0:
            connection[entry] =1
    connectionlevel = sum(connection)*1.0/len(connection)*1.0
    print(connectionlevel)
    f= open("%s/connectionlevel.txt"%dirsti,'a')
    f.write("\nnow checking data from test %s\n"%dirsti)
    f.write("connection level was %s\n"%connectionlevel)
    f.close()
    
    
connectivity("test","array.txt")