from string import printable


def xor(c1,c2):
	return chr(ord(c1)^ord(c2))

def update(cipher,key,plain):#Xor cipher with key, or cipher with key
	for i in range(len(cipher)):
		if key[i] != None and plain[i] != None:
			if not xor(key[i],plain[i])==cipher[i]:
				return False			
		elif key[i] == None and plain[i] !=None:
			c = xor(cipher[i],plain[i])
			if c in printable : key[i] = c 
			else :return False
		elif key[i] != None and plain[i] == None:
			c = xor(cipher[i],key[i])
			if c in printable : plain[i] = xor(cipher[i],key[i])
			else : return False
			
	return True

def updateKeyPlain(key,plain,lenkey):
	#newkey = plain
	lenplain = len(key) -lenkey
	for i in range(lenkey):
		try :
			j = i+lenplain
			if plain[j] == None and key[i] != None : 
				plain[j] = key[i]
			elif plain[j] != None and key[i] == None:
				key[i] = plain[j]
			elif plain[j] != None and key[i] == None:
				#assert key[i] == plain[j]
				if not key[i] == plain[j]:return False
		except IndexError:
			return
	return True

def updateKey(key,lenkey):#
	try : 
		for i in range(lenkey):
			j = i + lenkey
			if key[i]==None and key[j]!=None:
				key[i] = key[j]
			elif key[i]!= None and key[j] == None:
				key[j] = key[i]
			elif key[i] != None and key[j]!=None:
				#assert key[i] == key[j]
				if not key[i] == key[j] : return False
				
	except IndexError:
		return True 
	return True
		

	
	


#Init cipher plain key
encrypted = open("encrypted").read().decode('hex')
cipher = list(encrypted[:-32])
L = len(cipher) #105


for  lenkey in range(6,99):
	#lenkey = 67
	print "lenkey ",lenkey
	lenplain = L-lenkey
	
	plain = []
	key = []
	for i in range(L):
		plain.append(None)
		key.append(None)

	#Remplir les info
	knowtext = "flag{"
	for i in range(len(knowtext)):
		plain[i] = knowtext[i]

	for j in range(20):
		if not update(cipher,key,plain) : break
		if not updateKeyPlain(key,plain,lenkey) : break
		if not updateKey(key,lenkey) : break
	if None not in plain : break

print
print "".join(plain)
print "".join(key)

