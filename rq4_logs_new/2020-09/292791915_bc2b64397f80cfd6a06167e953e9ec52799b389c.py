from random import * 




#Exercice 1 

def volume(r,h):
    res=(3.14*r**2*h)/3
    return res 

#print(volume(14,12))


#Exercice 2

def jeux(t):
    """ 't' est le nombre de coups maximum que l'on peut mettre sur le jeux 
       (un grand nombre pour que ce soit infini et un très petit pour plus de difficulté)"""
    nb=randint(1,100)
    win=0
    c=0
    while c<t and win!=1:
        c=c+1
        res=int(input("donne un nombre : "))
        if res==nb:
            print("c'etait le nombre",res,"tu a gagner en",c,"coups")
            win=win+1
        if res<nb:
            print("c'est trop petit")
        elif res>nb:
            print("c'est trop grand")
    if t>=10:
        print("tu a perdu, le nombre était",nb) 
    return ""

#print(jeux(10))




#Exercice 3
        
def sapin(x):
    for i in range(1, x + 1, 2):
        print((i * "*").center(x))
    return ""

#print(sapin(50))
    




#Exercice 4

"""rep=1   
while rep!=0:
   ttc=rep*1.2
   rep=int(input("donner la valuer de votre nombre hors taxe ou 0 si vous l'avez déjà saisi (le nombre saisi est : "+str(rep)+" )  :  "))
print("votre prix ttc est de "+str(ttc)+" euros")"""


#Exercice 5

def chasseur(p,c,v,a):
    k=p*1+c*3+v*5+a*10
    print("vous avez utilisez",k,"points")
    k=k//100
    res=k*200
    print("vous devez payez",res,"€ pour",k,"permis")
    return ""

#print(chasseur(53,12,3,10))
    

#Exercice 6

"""nb=0
p=int(input("poids de Haruchi : "))
nouriture=int(input("quantité de nourriture que Haruchi mange par jour : "))
nb_animaux=int(input("nombre d'animaux : "))
for i in range(nb_animaux):
    res=input("animaux, liste des caractéristiques des animaux : ")
    list=[] 
    for i in res.split(" "):
        list.append(i)
    if (int(list[1])/int(list[0]))>(nouriture/p):
        nb=nb+1
        
print(nb)"""