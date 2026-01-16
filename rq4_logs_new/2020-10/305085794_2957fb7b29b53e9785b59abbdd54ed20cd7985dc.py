import re 
f = open("cedict_ts.u8",encoding="utf8")
fileRead=f.read()
fileRead_original=fileRead
f.close()



fileRead=re.sub('#.*\n','',fileRead)
fileRead=re.sub(' \[.*','',fileRead)



matrix = [line.split(' ') for line in fileRead.split('\n')]

total_count=len(matrix)
oneword_count=0
twoword_count=0
for line in matrix:
    if(len(line[1])==1):
        oneword_count += 1
    if(len(line[1])==2):
        twoword_count += 1
    
print("total count : " + str(total_count))
print("relative one char word: " + str(oneword_count /total_count))
print("relative two char word: " + str(twoword_count /total_count))