# use https://asecuritysite.com/Encryption/rsa for generating p,q,e values
# only can encrypt and decrypt small letters , no special characters like spaces
p=input('enter first prime number(p): ')
q=input('enter second prime number(q): ')
e=input('enter the value of e: ')
d=1
s=0
n=p*q
phi=(p-1)*(q-1)
while(s!=1):
	s=(d*e)%phi
	d+=1
d-=1
print 'd value is : '+str(d)
data=raw_input('enter the data to be encrypted: ')
print "encrypting ----------------------------------------"
enc=[]
for k in data:
	l=ord(k)-97
	c=1
	for m in range(0,e):
		c*=l
		c%=n
	c%=n
	enc.append(c)
print 'encrypted data is: '+str(enc)		
print "decrypting ----------------------------------------"
dec=[]
for k in enc:
	c=1
	for l in range(0,d):
		c*=k
		c%=n
	c%=n
	dec.append(chr(c+97))
str=' '
for k in dec:
	str+=k
print 'decrypted data is: '+str[1:]