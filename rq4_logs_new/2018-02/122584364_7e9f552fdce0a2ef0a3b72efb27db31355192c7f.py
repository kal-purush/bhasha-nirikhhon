
with open("input.txt") as ins:
    fields = []
    for line in ins:
        if line[0]!='0' and line[2]!='0':
            if line[0]!='*' and line[0]!='.':
                field=[]
                fields.append(field)
            else:
                field.append(list(line[:-1]))
        else:
            break

for file in fields:
    for m in range(len(file)):
        for n in range(len(file[m])):
            square=0
            if file[m][n]=='*':
                continue;
            else:
                if m!=0:
                    if(file[m-1][n]=='*'):
                        square=square+1
                    if n!=0:
                        if(file[m-1][n-1]=='*'):
                            square=square+1
                    if n<len(file[m])-1:
                        if(file[m-1][n+1]=='*'):
                            square=square+1
                if m<len(file)-1:
                    if(file[m+1][n]=='*'):
                        square=square+1
                    if n!=0:
                        if(file[m+1][n-1]=='*'):
                            square=square+1
                    if n<len(file[m])-1:
                        if(file[m+1][n+1]=='*'):
                            square=square+1
                if n!=0:
                    if(file[m][n-1]=='*'):
                        square=square+1
                if n<len(file[m])-1:
                    if(file[m][n+1]=='*'):
                        square=square+1            
            file[m][n]=square

fieldNumber=1
for file in fields:
    print 'Field #'+str(fieldNumber)+':'
    for m in range(len(file)):
        eachLine=''
        for n in range(len(file[m])):
            eachLine=eachLine+str(file[m][n])
        print eachLine
    print ""
    fieldNumber=fieldNumber+1
            
           
        
            