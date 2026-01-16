# s='e'
# for c in s:
#     if c.islower():
#          print c
# if s.islower():
#     print "yes"+ s
#
# s='R'
# if s.isupper():
#     print "Yes" + s
#
# s = raw_input('Type a word')
# l = []
# for c in s.strip():
#     if c.islower():
#         print c
#         l.append(c)
# print 'Total number of lowercase letters: %d'%(len(l))
#
# #check for alpha numeric
# if '123abc'.isalnum():
#     print "TRUE"
# s='123'  # you must convert number to the string before test of digit..
# if s.isdigit():
#     print "NUM"
# s1=int(s)
# print s1+10
#
#46.	A sentence splitter is a program capable of splitting a text into sentences Exercise
f= open('ashok201.txt','r');
g=open('ashok202.txt','w')
data = f.readlines()
data1=str(data)
str1=""
for a,b in enumerate(data1):
    if b.isupper():
        if a < 5:
            str1=""  # garbage before first capital letter will be removed.
            pass
        else:
            if ( data1[a-1]==" " and data1[a-2]=="."):
                if (data1[a-4])=="M" and (data1[a-3]=="r" or"s"):
                    pass
                else:
                    if( data1[a-1]==" " and data1[a-2]=="."):
                        print str1
                        g.writelines([str1, '\n'])
                        str1=""
    str1=str1+b
# the below code will clean the last string in the text.truncate after full stop
for e in range((len(str1)-1),1,-1):
    if str1[e]==".":
        str2= str1[0:e+1]
        print str2
        g.writelines(str2)
        break
f.close()
g.close()

