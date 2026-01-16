# in this program it will automatically generate e value based on p and q
from fractions import gcd
from random import randint
es=[]
def selecterandom(p,q):
	phi=(p-1)*(q-1)
	for i in range(3,phi):
		if(gcd(i,phi)==1):
			es.append(i)
	print 'list of possible e values: '+str(es)	
	i=randint(0,len(es)-1)
	print 'randomly selected e value: '+str(es[i])
	return es[i]

p=input("enter p :")
q=input("enter q :")
n=p*q
phi=(p-1)*(q-1)
e=selecterandom(p,q)
d=1
s=0
while(s!=1):
	s=(d*e)%phi
	d+=1;
d-=1
print("public key "+str(e)+" "+str(n))
print("private key "+str(d)+" "+str(n))

data=raw_input("enter the data to be encrypted : ")
list=[]
for k in data:
	list.append(ord(k)-97)

print("encryption -----------------------------------------")
enc=[]
for k in list:
	c=1
	for m in range(0,e):
		c=c*k
		c=c%n
	c=c%n
	enc.append(c)
print 'encrypted data : '+str(enc)
print("decryption ----------------------------------------------")
dec=[]
for k in enc:
	c=1
	for m in range(0,d):
		c=c*k
		c=c%n
	c=c%n
	dec.append(chr(c+97))
print 'decrypted data is: '+''.join(dec)
