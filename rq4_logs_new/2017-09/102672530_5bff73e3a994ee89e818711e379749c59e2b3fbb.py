#hw1.py -- HW1DB, simple database implementation in python. a CLI that interfaces with csv files given as arguments

import sys
import csv
from os import system, path



def parseColumns(columns,kv=True):
    spl = columns.split(',')
    ans = [spl[0]]
    for x in range(1,len(spl)):
        if len(ans[-1]) > 0 and ans[-1][-1] != '\"' and ('=\"' in ans[-1] or (not kv and ans[-1][0] == '\"')):
            ans[-1] = ans[-1] + ',' + spl[x]
        else: 
            ans.append(spl[x])
    return ans

def checkColumnNames(tablename, columns):
    with open(metas[tablename][0]) as table:
        r = csv.DictReader(table)
        head = r.next()
        for c in columns:
            try:
                val = head[c]
            except KeyError:
                print 'Error: ' + c + ' is not a column of table ' + tablename
                return False
    return True

def find(tablename, cols):
    colvals = parseColumns(cols)
    print colvals
    cvs = dict()
    for val in colvals:
        try:
            [name,value] = val.strip().split('=',1)
            if name in cvs: 
                print 'Error: cannot have two values for the same column'
                return
            cvs[name] = value
        except ValueError:
            print 'Error: please specify columns in the form of column1=value1'
            return #non-fatal
            
    if not checkColumnNames(tablename,cvs.keys()):
        #returned false, must be error
        return
         
    print cvs
    with open(metas[tablename][0]) as table:
        r = csv.DictReader(table)
        for row in r:
            match = True
            for k in cvs:
                if row[k] != cvs[k].strip('\"'):
                    match = False
                    break
            if match: print row
   
    
def insert(tablename, data):
    with open(metas[tablename][0]) as table:
        r = csv.DictReader(table)
        ncols = len(r.next())
        if ncols != len(parseColumns(data,False)):
            print 'Error: Data does not contain correct amount of columns'
            return
    with open(metas[tablename][0],'a') as table:
        table.write(data.strip('()'))
        
    
    '''
    vals = data.strip('()').split(',')
    cvs = dict()
    print vals
    #system('echo ' + a + ' >> ' + c)'''


#input check
if len(sys.argv) < 2:
    print 'ERROR: please specify CSV files as arguments in the form of alias1=csv1.csv [...aliasN=csvN.csv]'
    sys.exit(6)

metas = dict() #contains metadata for each csv (filename, headers)

for arg in sys.argv[1:]:
    try: 
        [a,b] = arg.split('=')
        if a == '' or b == '' or a in metas:
            print 'ERROR: Cannot have blank filename or alias. cannot reuse aliases.'
            sys.exit(6) #fatal
        if not path.isfile(b):
            print 'ERROR: ' + b + ' is not a file'
            sys.exit(6)
        metas[a] = [b]
    except ValueError: 
        print 'ERROR: please specify arguments in the form of alias1=csv1.csv [...aliasN=csvN.csv]'
        sys.exit(6) #fatal, give descriptive error message
        
print metas
'''
for key in metas: 
    with open(metas[key][0]) as table:
        reader = csv.DictReader(table)
        for row in reader:
            print row['colA']
'''


while 1:
    x = raw_input("HW1DB> ")
    if x.upper() == 'EXIT': break
    splits = x.split(' ',2)
    if splits[0].upper() not in ['FIND','INSERT'] or len(splits) != 3: 
        print 'ERROR: command not recognized'
        continue
    if splits[1] not in metas:
        print 'ERROR: table ' + splits[1] + ' does not exist'
        continue
    #print splits
    if splits[0].upper() == 'FIND':
        find(splits[1],splits[2])
    else: 
        insert(splits[1],splits[2])

    
    
    
    
    
    
    
    
    
    
    
    
    