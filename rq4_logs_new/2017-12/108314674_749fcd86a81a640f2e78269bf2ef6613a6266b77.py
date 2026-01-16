import math
import scipy.signal
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import interp1d
from statistics import median
from Load_file import get_values_from_file
from Pan_tompkins import stworz_wykres
import Signal_processing as sgl
import sys

nazwa_pliku='sygnal.txt'
[Time, Resp, BPL, ECG] = get_values_from_file(nazwa_pliku)
x=scipy.signal.detrend(ECG)
x=x/max(abs(x))
x1=x
[ind_p, ind_n, Time, q, r, s]=stworz_wykres(nazwa_pliku)
N1=len(ind_n)

p11=plt.plot(Time, x1, '-')
plt.xlim([0,5])
plt.show()

for n in range(N1): #petla po wszystkich wykrytych QRSach 
   x[ind_p[n]:ind_n[n]]=0 #kasowanie wykresu w miejscach QRSu
   if n==0: 
       x[0:ind_p[n]]=x[0:ind_p[n]]-min(x[0:ind_p[n]])#dosuwanie do osi x odcinka od zero do pierwszego QRSa
       m=median(x[0:ind_p[n]])
       x1[0:ind_p[n]]=x[0:ind_p[n]]-m	#odejmowanie mediany(z pdf QRS_WAVES)
   elif n>0: 
       x[ind_n[n-1]:ind_p[n]]=x[ind_n[n-1]:ind_p[n]]-min(x[ind_n[n-1]:ind_p[n]]) #dosuwanie do osi x wykresu na odcinku od końca poprzedniego QRSa do początku następnego
       m=median(x[ind_n[n-1]:ind_p[n]])
       x1[ind_n[n-1]:ind_p[n]]=x[ind_n[n-1]:ind_p[n]]-m		#odejmowanie mediany(z pdf QRS_WAVES)

max=max(x1)
for i in range(len(x1)):
    if x1[i]<0.07*max:
        x1[i]=0		#zerowanie mniejszych niz 0,07 max
    else:
        continue
# średnia
Esi=np.mean(x1)
index_pos=[0]*len(x1)
for i in range(len(x1)):
	if x1[i]>x1[i]*Esi:
		index_pos[i]=1 #.append
	else:
		continue
#p2=plt.plot(Time, sd, Time, index_pos)
#plt.show()

Delay=16 #Empirycznie dopasowane (Nie wiem skąd brali do QRSa)
Delay2=12 #Empirycznie dopasowane (Nie wiem skąd brali do QRSa)
Delay1=10
'''p3= plt.plot(Time,x1,Time,index_pos ) #-Time[Delay-1]
plt.xlim([0, 3])
plt.show()
'''
# DETEKCJA P i T
# pochodna index_pos-mówi kiedy rośnie (1) a kiedy maleje (-1)
d_index_pos=np.diff(index_pos)
ind_p = []
ind_n = []
for i in range(len(d_index_pos)):
	if d_index_pos[i] == 1:
		ind_p.append(i) #indeks początków załamków
	elif d_index_pos[i] == -1:
		ind_n.append(i) # indeks końców załamków

p,t=[],[]
N=len(ind_n)
p1,p2,t1,t2=np.zeros((2,N)),np.zeros((2,N)),np.zeros((2,N)),np.zeros((2,N))
	
if ind_n[0]<ind_p[0]: #jakby się zaczynało od środka górki
	ind_n.remove(ind_n[0])
	ind_p.pop()
for i in range(len(ind_n)):		#po pełnych załamkach
	if i>0 and ind_n[i]>ind_p[i]:
		for n in range (ind_p[i],ind_n[i]): #po załamku bieżącym WYKRYWANIE MAKSIMUM, funkcją max nie działa
			if n>0:
				if x1[n]>x1[n-1]:
					m=x1[n]
			#print(m)
		for j in range (ind_p[i-1],ind_n[i-1]): #po załamku poprzednim WYKRYWANIE MAKSIMUM, funkcją max nie działa
			if j>0:
				if x1[j]>x1[j-1]:
					m1=x1[j]
			#print(m1)	
		if m>m1: #spr czy pierw załamek p potem t
			t.extend([(ind_p[i]-Delay)/fs,(ind_n[i]-Delay)/fs])	#dopisuje początek i koniec załamka t
			p.extend([(ind_p[i-1]-Delay)/fs,(ind_n[i-1]-Delay)/fs]) #dopisuje początek i koniec załamka p
			p1[0][i] = Time[ind_p[i-1]-Delay]
			p1[1][i] = ECG[ind_p[i-1]-Delay]
			t1[0][i] = Time[ind_p[i]-Delay]
			t1[1][i] = ECG[ind_p[i]-Delay]
			p2[0][i] = Time[ind_n[i-1]+Delay1]
			p2[1][i] = ECG[ind_n[i-1]+Delay1]
			t2[0][i] = Time[ind_n[i]+Delay2]
			t2[1][i] = ECG[ind_n[i]+Delay2]
			
for i in range(0,len(p),2):
	print ("Początek załamka p:",p[i],"Koniec załamka p:",p[i+1])
for i in range(0,len(t),2):
	print ("Początek załamka t:",t[i],"Koniec załamka t:",t[i+1])	

p11=plt.plot(Time, ECG, '-',p1[0],p1[1],'^g',p2[0],p2[1],'^g',t1[0],t1[1],'^r',t2[0],t2[1],'^r')
plt.xlim([0,5])
plt.show()

