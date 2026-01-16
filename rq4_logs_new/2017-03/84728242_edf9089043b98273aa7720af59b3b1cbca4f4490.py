#!/bin/python3.6
import re

phase1=[0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, -1, -1, -1, -1, -1, 0]
phase2=[-3,-10,-1,-15, -8,-50,-62,-40,  0,  0,  0,  0,  0,  0, 0]
Z=phase1


varIn=[0]
varOut=[0]

simplex_table=[
[20, 5,  7,  3,  9, 0.5,  0,  0, -1,  0,  0,  0,  0,  0,  1,  0,  0,  0,  0,    5],
[1,  2,  0, 15,  5,  10,  0, 20,  0,  0,  0,  0,  1,  0,  0,  0,  0,  0,  0,    3],
[1,  2,  0, 18,  2,   9, 25, 12,  0, -1,  0,  0,  0,  0,  0,  1,  0,  0,  0,    1],
[1, 12,  3,  0,  2,  15, 15, 25,  0,  0, -1,  0,  0,  0,  0,  0,  1,  0,  0,    2],
[0,  6,  0,  5,  1,  20, 25, 25,  0,  0,  0, -1,  0,  0,  0,  0,  0,  1,  0, 6.66],
[5, 13, 10,  5,  6,   0,  0,  0,  0,  0,  0,  0,  0,  1,  0,  0,  0,  0,  0,    2],
[1,  1,  1,  1,  1,   1,  1,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,    1],
[0,  0,  0,  0,  0,   0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,    0]
]#Z Indicatrice

Base=['A1', 'S1', 'A2', 'A3', 'A4', 'S2', 'A5']
Vars=['l', 't', 'r', 'y', 'd', 'm', 'i', 'h', 'E1', 'E2', 'E3', 'E4', 'S1', 'S2', 'A1', 'A2', 'A3', 'A4', 'A5']

def removeCol(i):
	for row in simplex_table:
		row.pop(i)

def removeA():
	p = re.compile("A.*");
	for i in Vars:
		if p.match(i) != None :
			removeCol(Vars.index(i))
			Vars.pop(Vars.index(i))

def rezZ():
	for i in range(0,len(simplex_table[len(simplex_table)-1])):
		simplex_table[len(simplex_table)-1][i]=0

def comptZ():
	for i in range(0,len(Z)):
		simplex_table[len(simplex_table)-1][i]-=Z[i]

	for i in range(0, len(simplex_table)-1):
		coef=Z[Vars.index(Base[i])]
		for j in range(0,len(Z)) :
			simplex_table[len(simplex_table)-1][j]+=coef*simplex_table[i][j]
	
def Stop():
	cont = False
	cur=0
	for i in simplex_table[len(simplex_table)-1]:
		if i < 0 and cur != len(simplex_table[len(simplex_table)-1])-1 : 
			cont = True
			break
	return cont

def findIN():
	tmp=0
	cur=0
	for i in simplex_table[len(simplex_table)-1] :
		if i < tmp and cur != len(simplex_table[len(simplex_table)-1])-1 :
			tmp=i
			varIn[0]=cur
		cur+=1

def findOUT():
	rve=0
	cur=0

	#Init tmp un peu sale mais sa feras l'affaire
	tmp = 10**100
	for row in simplex_table:
		if cur != len(simplex_table)-1:
			try :
				rve=row[len(row)-1]/row[varIn[0]]
				if rve<tmp and rve > 0:
					tmp = rve
					varOut[0] = cur
			except :
				pass
			cur+=1

def MajTab():
	coef=0
	r_cur=0
	pivot=simplex_table[varOut[0]][varIn[0]]

	for row in simplex_table :
		coef=-row[varIn[0]]/simplex_table[varOut[0]][varIn[0]]
		for i in range(0,len(row)) :
			if r_cur != varOut[0] :
				row[i]+=simplex_table[varOut[0]][i]*coef
		r_cur+=1
	for i in range(len(simplex_table[varOut[0]])):
		simplex_table[varOut[0]][i]/=pivot
	

def simplexe():

	while Stop():
		findIN()
		findOUT()
		Base[varOut[0]] = Vars[varIn[0]]
		MajTab()
	

##### MAIN
comptZ()
simplexe()
removeA()
rezZ()
Z=phase2
comptZ()
simplexe()

for i in range(len(Base)):
	print(Base[i],' : ',simplex_table[i][len(simplex_table[i])-1]) 




