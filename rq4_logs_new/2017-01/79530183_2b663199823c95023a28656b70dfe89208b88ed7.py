import operator
import copy
#count keys in D
def getC(D,keys):
	C=[]
	for key in keys:
		c=0
		for T in D:
			have=True
			for k in key:
				if k not in T:
					have=False
			if have:
				c+=1
		C.append(c)
	return C

#cut the keys with count less than minSup*length
def getCutKeys(keys,C,MS,length):
	for i,key in enumerate(keys[:]):
		if float(C[i])/length<MS[key[0]]:
			keys.remove(key)
	return keys

def level2_gen(L,phi):
	C2=[]
	C=getC(D,L)
	length=len(D)
	for num1,l in enumerate(L):
		if C[num1]*1.0/length>=MS[l]:
			for num2,h in enumerate(L[num1+1::]):
				if C[num2]*1.0/length>=MS[l] and abs(MS[l]-MS[h])<=phi:
					C2.append([l,h])
	return C2

def apriori_gen(keys1,Last_keys,phi):
	keys2=[]
	if [] in keys1:
		keys1.remove([])
	for i,k1 in enumerate(keys1[:]):
		for j,k2 in enumerate(keys1):
			length=len(k1)
			if i<j and abs(MS[k1[length-1]]-MS[k2[length-1]])<=phi:
				key=[]
				Same=True
				for l in range(length-1):
					if (k1[l]!=k2[l]):
						Same=False
					key.append(k1[l])
					

				
				if Same==True and (k1[length-1]!=k2[length-1]):
					c=k2[length-1]
					key.append(k1[length-1])
					key.append(c)
					if key not in keys2:
						keys2.append(key)
					
					for i1,word1 in enumerate(key):
						rem=[]
						for j1,word2 in enumerate(key):
							if j1!=i1:
								rem.append(word2)
						if i1!=0 or MS[key[1]]==MS[key[0]]:
							if rem not in Last_keys:
								if key in keys2:
									keys2.remove(key)

					
	return keys2

def MS_Sort(MS):
	MS_copy=copy.copy(MS)
	sort=sorted(MS_copy.iteritems(),key=operator.itemgetter(1))
	MS_sorted=[item[0] for item in sort]
	return MS_sorted





def Apriori(D,L,phi):
	cutKeys1=level2_gen(L,phi)

	keys=cutKeys1
	all_keys=[]
	Last_keys=keys[:]
	while keys!=[]:
		C=getC(D,keys)
		cutKeys=getCutKeys(keys,C,MS,len(D))
		if [] in cutKeys:
			cutKeys.remove([])
		if len(cutKeys)==0:
			break
		for k in cutKeys:
			all_keys.append(k)
		keys=apriori_gen(cutKeys,Last_keys,phi)
		Last_keys=keys[:]

	return all_keys



D = [['A','B','C','D'],['B','C','E'],['A','B','C','E'],['B','D','E'],['A','B','C','D']]
MS={'A':0.7,'B':0.7,'C':0.5,'D':0.6,'E':0.1}
L=MS_Sort(MS)
F=Apriori(D,L,1)
print '\nfrequent itemset:\n',F