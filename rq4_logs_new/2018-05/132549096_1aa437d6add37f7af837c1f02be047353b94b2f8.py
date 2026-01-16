#!/usr/bin/python
from ete2 import Tree#,TreeStyle,NodeStyle
import sys
import re

def is_clade(n,tt):
	'''
	check if all descendants of node n contain the tag
	'''
	kids=n.get_leaf_names()
	res=1
	for name in kids:
		if not re.match(tt,name):
			res = 0

	return res

def is_foreign(n,tt,tag):
	'''
	for a polyphyletc cluster it finds all tags of the immigrants,
	n node
	tt: NUMBER of "local" tag
	taf: list of all tags among which to look for the immigrants
	'''
	
	kids = n.get_leaf_names()
	imm = []
	for name in kids:
		if not re.match(tag[tt],name):
			for t in tag:
				if re.match(t,name) and t not in imm:
					imm.append(t)
	return imm
	

def tmrca(l,t):
	r = t.get_common_ancestor(l)
	S = 0.0
	for nm in l:
		S += r.get_distance(nm)
	return S/l.__len__()

def tmrca_m(l,t):
	r = t.get_common_ancestor(l)
	S = []
	for nm in l:
		S.append(r.get_distance(nm))
	return max(S)	

def tmrca_min(l,t):
	r = t.get_common_ancestor(l)
	S = []
	for nm in l:
		S.append(r.get_distance(nm))
	return min(S)

def main():
	try:
		t=sys.argv[1]
		tt = sys.argv[2]
	except:				
		print( 
		'''
	==============================================   
			USAGE:
	==============================================
		Two filenames are required
		as argumets:
		1) treefile containing mitochondrial tree
		2) treefile containing nuclear tree
		
	==============================================
		'''
		)
		sys.exit()
	tag = []
	ntags = 0
	try:
		with open("tagfile") as tf:
			for line in tf:
				tag.append(line.rstrip('\n'))
				ntags += 1
	except:
		print(
		'''
	===============================================
		You need file called tagfile in the same 
		directory as the data. It contains tags 
		used to distinguish between the groups. 
		Tags must be valid REGEX expressions, 
		one tag per line
		
		EXAMPLE:
		^B OTU names begin with "B"
		1A OTU names contain "1A" somewhere

	!!! IMPORTANT !!!
	There should be no empty line ant the bottom 
	of the tagfile

	================================================ 
		'''
		)
		sys.exit()
	
	try:
		t1=Tree(sys.argv[1],format=0)
	except:
		print "\n\tWrong or absent file %s!\n"%sys.argv[1]
		sys.exit()
	try:
		t2=Tree(sys.argv[2],format=0)
	except:
		print "\n\tWrong or absent file %s!\n"%sys.argv[2]
		sys.exit()
	
	nms1 = t1.get_leaf_names() #names from the 1st tree
	nms2 = t2.get_leaf_names() #names from the 2nd tree
	
	#There may be more than 2 groups! We do as many as there are in the tagfile
	#Still we need only two tags to obtain full stat! 

	grps = []
	grps1 = []
	for g in enumerate(tag):
		tmp = []
		tt = []
		grps.append(tmp) 
		grps1.append(tt)
	
	ntagn = tag.__len__() #number of tags
	for i in range(0,ntagn):
		for nm in nms1:
			if re.match(tag[i],nm):
				grps[i].append(nm)
		if grps[i].__len__()<1:
			print "group of %s in the mitochondrial tree is empty! Quitting"%tag[i]
			sys.exit()
		else:
			print "group of %s in the mitochondrial tree consists of %i OTU"%(tag[i],grps[i].__len__())
		#second tree
	for i in range(0,ntagn):
		for nm in nms2:
			if re.match(tag[i],nm):
				grps1[i].append(nm)
		if grps[i].__len__()<1:
			print "group of %s in the nuclear tree is empty! Quitting"%tag[i]
			sys.exit()
		else:
			print "group of %s in the nuclear tree consists of %i OTU"%(tag[i],grps[i].__len__())
	ca = [] #common ancestors for species
	ca1 = []
	
	for i in range(0,ntagn):
		ca.append(t1.get_common_ancestor(grps[i]))
#	for i in range(0,ntagn):
		ca1.append(t2.get_common_ancestor(grps1[i]))
	print "Clade is monopyletic (1/0), 1st column is first tree"
	pphyl0 = []
	pphyl1 = []
	
	print "--------------------------------------------------"
	for i in range(0,ntagn):
		ind = [0,0]
		ind[0] = is_clade(ca[i],tag[i])
		ind[1] = is_clade(ca1[i],tag[i])
		if ind[0] == 0:
			pphyl0.append(i)
		if ind[1] == 0:
			pphyl1.append(i)
		print("Tag:\t%s\t %i\t%i"%(tag[i],ind[0],ind[1]))
	print "--------------------------------------------------\n"	
	
	print "\nDistance from common ancestor of a clade to"
	print "the common root of the two clades (CA_x/ROOT)"
	print "----------------------------------------------------------------"
	
	for i in range(0,ntagn-1):
		for j in range(i+1,ntagn):
			d1=t1.get_distance(ca[i],t1.get_common_ancestor(ca[i],ca[j]))
			d2=t1.get_distance(ca[j],t1.get_common_ancestor(ca[i],ca[j]))
			d3=t1.get_distance(ca[i],ca[j])
			print "Tags:\t %s/%s\t%f\t%f\t%f"%(tag[i],tag[j],d1,d2,d3)
		for j in range(i+1,ntagn):
			d1=t2.get_distance(ca1[i],t2.get_common_ancestor(ca1[i],ca1[j]))
			d2=t2.get_distance(ca1[j],t2.get_common_ancestor(ca1[i],ca1[j]))
			d3=t2.get_distance(ca1[i],ca1[j])
			print "Tree2:\t %s/%s\t%f\t%f\t%f"%(tag[i],tag[j],d1,d2,d3)
	print "----------------------------------------------------------------"
	print "\n"
	if pphyl0.__len__() ==0 and pphyl1.__len__()==0:
		print "THERE ARE NO POLYPHYLETIC GROUPS!"
		sys.exit()
	
	else:
		for c in pphyl0:
			cn = ca[c].get_leaf_names()
			print cn
			others = []
			for n in cn:
				if not re.match(n,tag[c]):
					others.append(n)
			dt = t1.get_distance(t1.get_common_ancestor(cn),t1.get_common_ancestor(others))
			print "tree1: distance between CA of %s and CA of immigrants is %f"%(tag[c],dt)
		for c in pphyl1:
			cn = ca1[c].get_leaf_names()
			others = []
			for n in cn:
				if not re.match(n,tag[c]):
					others.append(n)
			dt = t2.get_distance(t2.get_common_ancestor(cn),t2.get_common_ancestor(others))
			print "tree2: distance between CA of %s and CA of immigramts is %f"%(tag[c],dt)		
	sys.exit()

if __name__=='__main__':
    main()
	