#! /usr/bin/python
# _*_ coding:utf-8 _*_

x=[]
x.extend([1,2])
# Sorting built-in
L = ['abc', 'ABD', 'AbE']
x[0] = sorted(L, key=str.lower, reverse=True)

# Pretransform items: differs!
L = ['abc', 'ABD', 'AbE']
x[1] = sorted([s.lower() for s in L], reverse=True)
for i in x:
	print(i)