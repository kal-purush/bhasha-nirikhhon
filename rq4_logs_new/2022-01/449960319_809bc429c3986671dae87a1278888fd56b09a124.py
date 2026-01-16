
import re
dic={'username':'Amaljith','password':'Samaljith@95', 'email':"amaljith95@gmail.com"}

user=[value for value in dic.values()][0]
match=re.search(r'[A-Z]\w+',user)
if match:
    I1=(match.group())
    ID1=I1[0]
    
pswd=[value for value in dic.values()][1]    
match=re.search(r'.+',pswd)
if match:
    I2=match.group()
    ID2=I2[0]

mail=[value for value in dic.values()][2] 
match=re.search(r'\w+@\w+',mail)
if match:
    I3=match.group()
    ID3=I3[0]
    
print("The user Id is",(ID1+ID2+ID3))
    
# data2="".join(dic.values())
# print(data2)