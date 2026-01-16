import os
import random
path=os.getcwd()
key="abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+}{:<>?"
while(2>1):
	random = ''.join([random.choice(key) for n in xrange(32)])
	path=path+'/'+random
    	os.makedirs(path)
	