import random as r
import numpy as np
def saisir_taille():
    taille=int(input("donner la taille"))
    if 0<taille<20 :
        return taille
    else :
        return saisir_taille(taille)
    

def remplir_tab(taille,t):
    h=taille*taille
    i=0
    new=r.randint(0,20)
    if t[i]!=h or verif()==False:
        i+=1
        t[i]=new
    elif verif()==True :
        return remplir_tab(taille,t)
    else :
        return t
    
    
def verif(t,i,new):
    n==0
    if t[n]>=i :
        n+=1
        return verif(t,i)
    elif new==t[i] :
        return True
    else :
        return False

    
def remplir_mat(t,taille,m):
    l=0
    c=0
    i=0
    if l<=taille :
        m[l,c]=t[i]
        i+=1
        return remplir_mat(t,taille,m)
    else :
        return m
t=np.zeros(20,dtype =int)
m= np.zeros([l,c], dtype=int)

print(remplir_mat(t,taille,m))