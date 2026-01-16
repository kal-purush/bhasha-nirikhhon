#!/usr/bin/python

seq=open('dataset_3_2.txt').read()
len=len(seq)
seq_c=[]
dict={'A':'T', 'T':'A', 'G':'C', 'C':'G'}
for i in range(len-2,-1,-1):
	b=seq[i]
	c=dict[b]
	seq_c.append(c)
print(''.join(seq_c))