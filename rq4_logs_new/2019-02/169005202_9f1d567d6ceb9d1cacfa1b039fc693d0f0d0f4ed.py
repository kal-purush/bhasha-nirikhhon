#!/usr/bin/python3
import locale
from dialog import Dialog
import os, fnmatch
import re
import time

def grep(pattern,word_list):
	expr = re.compile('.*?'+pattern)
	return [elem for elem in word_list if expr.match(elem)]

d=Dialog()
#d.set_backgound_title("find a password entry")

code, answer = d.inputbox("Input your query", init="")

if (code != "ok"):
	print(code)
	exit(1)

passdir=os.getenv("PASSWORD_STORE_DIR", os.getenv("HOME")+"/.password-store")
os.chdir(passdir)


# find password store entrys 
passwords=[]
for root, dirs, files in os.walk("."):
	for name in files:
		if (fnmatch.fnmatch(name, "*.gpg")):
			f=os.path.join(root, name)
			f=f[2:]
			f=f[:-4]
			passwords.append(f)

passwords.sort()

i=1
choices=[]
chdict={}
for p in grep(answer, passwords):
	istr=str(i)
	item=istr[-1:]+"_"+istr
	choices.append((item, p))
	chdict[item]=p
	i=i+1

code, answer = d.menu("Select remains", height=23, menu_height=18, width=76,
		choices=choices
	)

if (code != "ok"):
	print(code)
	exit(1)

print("")
print("")
os.system("pass -c "+chdict[answer])

time.sleep(47)
