#!/usr/bin/env python
# -*- coding: utf-8 -*-


import numpy as np
from matplotlib import pyplot as plt
from matplotlib import animation
from matplotlib import widgets as wg

#%matplotlib qt    #   /!\ inserer cette ligne de code si l'IDE n'affiche pas le graph dans une fenetre séparée (probleme rencontré avec SPIDER)


# Explications :
# On simule le Jeu de la vie, un automate cellulaire similaire a la fourmi de Langton avec des règles differentes.
# Des cellules évoluent dans le monde et en fonction du nombre de voisins, elles vivent, meurent, ou donnent naissance à d'autres cellules.
# Cet automate permet de créer des structures très complexe avec peu de cellules au départ, le jeu est d'ailleurs turing complet.
# Dans ce programme, on choisis l'état initale en placant autant de cellules que l'on veut puis on voit l'évolution du monde
# On peut placer des cellules ou des structures préfrabriquées grâce aux boutons, l'emplacement est definit par les sliders
# On peut changer de dimensions du monde sans problème, dans la partie Initialisation ci dessous

print(" ==> Cliquez sur les boutons pour ajouter des cellules ou des structures de cellules.")
print(" ==> Les sliders definissent la position des structures.")
print(" ==> A gauche est montre l'etat de la grille après la dernière modification manuelle.")
print(" ==> CLiquez sur lancer l'animation quand vous souhaitez demarrer.")
print("     /!\ L'animation ne peut pas être stoppée une fois lancée.")
print("     Des structures peuvent être ajoutes après le debut de l'animation. Les cellules isolees meurent directement")
print("     /!\ Survolez les bouttons alors que l'animation est lancée ralentit la simulation! (A eviter)")
print("     Le Bloc chaos est une structe de 5 cases qui donne un comportement chaotique sur toute la grille.")
print("     Les blocs aléatoires ont effet sur toute la grille et ne sont pas affectés par les sliders.")


#Initialisation :

dim = 75  #tant qie dim >= 12, on peut changer la dimensions sans restriction théorique.
etat = np.zeros((dim, dim)) #état du monde, traité comme une variable global grace a etat[:] = ...

# état initial pour exemple :  
etat[10,11]=1
etat[9,11]=1
etat[8,11]=1
etat[9,10]=1

#fonctions de jeu  
 # règles : 
 # Une cellule vide à l'étape n − 1 et ayant exactement 3 voisins sera occupée à l'étape suivante. (naissance liée à un environnement optimal)
 # Une cellule occupée à l'étape n − 1 et ayant 2 ou 3 voisins sera maintenue à l'étape n sinon elle est vidée.

def nombre_voisins(etat,i,j):  #compte le nombre de voisins == 1  autour de la cellule (i,j) (la cellule se compte elle même si elle est activée)
    n=0
    for a in range (i-1,i+2):
        for b in range(j-1,j+2):
            if etat[a,b] == 1 :
                n=n+1
    return n
    


def evolution (etat) : # fait passer l'état du temps n au temps n+1
    dim=len(etat)
    etat_final=np.zeros((dim,dim)) #modifications que l'on apporte à l'etat
    for i in range(1,dim-1) :
        for j in range(1,dim-1) : # scan de toutes les cellules pour savoir quelle couleur on va lui attribuer
            if nombre_voisins(etat,i,j) == 3 and etat[i,j] == 0 : #si trois voisins -> naissance d'une cellule où il n'y avait rien
                etat_final[i,j] = 1
            if nombre_voisins(etat,i,j) in [3,4] and etat[i,j] == 1 : #si 2 ou 3 voisins -> cellule continue de vivre (on met 3,4 car la cellule se compte elle même)
                etat_final[i,j] = 1
    etat[:]=etat_final # modification de la case mémoire où pointe la variable etat, pour modifier etat meme si il n'est pas global
            

#affichage de l'interface utilisateur :
fig = plt.figure()
    #les siders pour positionner le curseur :
axsliderX=plt.axes([0.1,0.1,0.3,0.03])
sliderX = wg.Slider(axsliderX,"position x",valmin=1,valmax=dim-1,valstep=1,valinit=dim//2)
axsliderY=plt.axes([0.1,0.05,0.3,0.03])
sliderY = wg.Slider(axsliderY,"position y",valmin=1,valmax=dim-1,valstep=1,valinit=dim//2)

    #l'image de l'état initial
ax0 = fig.add_subplot(141) 
#fig.subplots_adjust(bottom=0.4)
ax0.title.set_text('état initial')
im0= ax0.imshow(etat, interpolation='none',cmap="gray", vmin=0, vmax=1)

    #boutons de mise en place de structures :
#(d'abord on positionne le bouton grâce à l'axe puis on creer le bouton, on lui affecte une fonction plus tard)
ax_but_suppr = plt.axes([0.1,0.15,0.1,0.03])
bouton_suppr = wg.Button(ax_but_suppr,'Supprimer case')

ax_but_zero = plt.axes([0.2,0.15,0.1,0.03])
bouton_zero = wg.Button(ax_but_zero,'Effacer tableau')

ax_but_start = plt.axes([0.3,0.15,0.1,0.03])
bouton_start = wg.Button(ax_but_start,'Lancer animation')

ax_but_alea0 = plt.axes([0.1,0.2,0.1,0.03])
bouton_alea0 = wg.Button(ax_but_alea0,'Grille aléatoire')

ax_but_alea1 = plt.axes([0.2,0.2,0.1,0.03])
bouton_alea1 = wg.Button(ax_but_alea1,'Colonnes aléatoire')

ax_but_chaos = plt.axes([0.3,0.2,0.1,0.03])
bouton_chaos = wg.Button(ax_but_chaos,'Bloc Chaos')

ax_but_canon = plt.axes([0.3,0.23,0.1,0.03])
bouton_canon = wg.Button(ax_but_canon,'Ajouter Canon')

ax_but_ligne = plt.axes([0.2,0.26,0.1,0.03])
bouton_ligne = wg.Button(ax_but_ligne,'Ajouter Ligne')

ax_but_vaisseau = plt.axes([0.2,0.23,0.1,0.03])
bouton_vaisseau = wg.Button(ax_but_vaisseau,'Ajouter Vaisseau')

ax_but_case = plt.axes([0.1,0.26,0.1,0.03])
bouton_case = wg.Button(ax_but_case,'Ajouter Cellule')

ax_but_clignotant = plt.axes([0.1,0.23,0.1,0.03])
bouton_clignotant = wg.Button(ax_but_clignotant,'Ajouter Clignotant')

ax_but_colonne = plt.axes([0.3,0.26,0.1,0.03])
bouton_colonne = wg.Button(ax_but_colonne,'Ajouter Colonne')

# affichage du jeu
ax1 = fig.add_subplot(122)
ax1.title.set_text('Jeu de la vie')
im = ax1.imshow(etat, interpolation='none',cmap="gray", vmin=0, vmax=1)

# fonction des boutons 
#(On créer les fonctions qui seront activées par les boutons)
def placer_case(val):
    dim=len(etat)
    j = sliderX.val # on selctionne i,j en fonction des valeurs des sliders
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i,j] = 1
    etat[:] = np.logical_or(etat,etat_final) #ajout de la nouvel structure sur l'état initial
    im0.set_data(etat) # on met a jour les dessins à affichés
    im.set_data(etat)
    plt.draw() #on dessine le nouvel affichage

def suppr_case(val):
    dim=len(etat)
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    if etat [i,j] == 1 : # on suprimme une cellule que si la destination est pleine
        etat_final[i,j] = -1
    etat[:] = np.add(etat,etat_final) #on suprimme la cellule en ajoutant -1 à 1
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def zero(val) :
    etat[:]=np.zeros(dim) #on remplit la matrice de zeros (cellule vide)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def alea0 (val) :
    p=0.4 #densité de celulles acives
    alea = np.random.random((dim,dim)) #on créer une matrice de chiffre aléatoire uniforme compris entre 0 et 1
    etat_final = (alea<=p).astype(int) #on transforme en True puis en 1 tous ceux qui sont <= p
    etat[:] = etat_final #la nouvelle matrice de 0,1 aléatoires devient notre nouvel état initial
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()


def alea1(val):
    p=0.5
    alea = np.random.random((dim,)) #pareil que aléa0 mais que sur un seul axe pour avoir des colonnes
    etat_final = (alea<=p).astype(int)
    etat[:] = etat_final
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def chaos(val) : #structure prédifinit qui a une évolution chaotique (au sens mathématique)
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i,j]  = 1
    etat_final[i+1,j]  = 1
    etat_final[i-1,j]  = 1
    etat_final[i,j+1]  = 1
    etat_final[i-1,j-1]  = 1
    etat[:] = np.logical_or(etat,etat_final)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def ligne(val) :
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i,j-3]  = 1
    etat_final[i,j-2]  = 1
    etat_final[i,j-1]  = 1
    etat_final[i,j]  = 1
    etat_final[i,j+1]  = 1
    etat_final[i,j+2]  = 1
    etat_final[i,j+3]  = 1
    etat_final[i,j+4]  = 1
    etat[:] = np.logical_or(etat,etat_final)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def colonne(val) :
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i+3,j]  = 1
    etat_final[i-2,j]  = 1
    etat_final[i-1,j]  = 1
    etat_final[i,j]  = 1
    etat_final[i+1,j] = 1
    etat_final[i+2,j]  = 1
    etat_final[i+3,j]  = 1
    etat_final[i+4,j]  = 1
    etat[:] = np.logical_or(etat,etat_final)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def vaisseau(val) : #structure qui se déplace
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i+2,j]  = 1
    etat_final[i+2,j-1]  = 1
    etat_final[i+1,j-3]  = 1
    etat_final[i+1,j+2]  = 1
    etat_final[i,j+3]  = 1
    etat_final[i-1,j+3]  = 1
    etat_final[i-1,j-3]  = 1
    etat_final[i-2,j-2]  = 1
    etat_final[i-2,j-1]  = 1
    etat_final[i-2,j]  = 1
    etat_final[i-2,j+1]  = 1
    etat_final[i-2,j+2]  = 1
    etat_final[i-2,j+3]  = 1
    etat[:] = np.logical_or(etat,etat_final)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def canon(val) :
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i+4,j+6]=1
    etat_final[i+3,j+4]=1
    etat_final[i+3,j+6]=1
    etat_final[i+2,j+2]=1
    etat_final[i+2,j+3]=1
    etat_final[i+2,j+16]=1
    etat_final[i+2,j+17]=1
    etat_final[i+2,j-5]=1
    etat_final[i+2,j-6]=1
    etat_final[i+1,j]=0
    etat_final[i+1,j+2]=1
    etat_final[i+1,j+3]=1
    etat_final[i+1,j+16]=1
    etat_final[i+1,j+17]=1
    etat_final[i+1,j-3]=1
    etat_final[i+1,j-7]=1
    etat_final[i,j+2]=1
    etat_final[i,j+3]=1
    etat_final[i,j-2]=1
    etat_final[i,j-8]=1
    etat_final[i,j-17]=1
    etat_final[i,j-18]=1
    etat_final[i-1,j+4]=1
    etat_final[i-1,j+6]=1
    etat_final[i-1,j-1]=1
    etat_final[i-1,j-2]=1
    etat_final[i-1,j-4]=1
    etat_final[i-1,j-8]=1
    etat_final[i-1,j-17]=1
    etat_final[i-1,j-18]=1
    etat_final[i-2,j-2]=1
    etat_final[i-2,j+6]=1
    etat_final[i-2,j-8]=1
    etat_final[i-3,j-7]=1
    etat_final[i-3,j-3]=1
    etat_final[i-4,j-6]=1
    etat_final[i-4,j-5]=1


    etat[:] = np.logical_or(etat,etat_final)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()

def clignotant(val) :
    j = sliderX.val
    i = sliderY.val
    etat_final = np.zeros((dim, dim))
    etat_final[i,j]=1
    etat_final[i+1,j]=1
    etat_final[i-1,j]=1
    etat_final[i,j-1]=1
    etat[:] = np.logical_or(etat,etat_final)
    im0.set_data(etat)
    im.set_data(etat)
    plt.draw()







#on lie les fonctions aux boutons

bouton_case.on_clicked(placer_case)
bouton_zero.on_clicked(zero)
bouton_suppr.on_clicked(suppr_case)
bouton_alea1.on_clicked(alea1)
bouton_alea0.on_clicked(alea0)
bouton_chaos.on_clicked(chaos)
bouton_ligne.on_clicked(ligne)
bouton_colonne.on_clicked(colonne)
bouton_vaisseau.on_clicked(vaisseau)
bouton_clignotant.on_clicked(clignotant)
bouton_canon.on_clicked(canon)


#fonction d'animation, elle est lancé grace à la variable anim, qui est créée quand on clique sur le bouton_start

def animate(i):
    evolution(etat) #itération de l'évolution
    im.set_data(etat) #mise à jour affichage

    return [im]

def starter (val) :  
    global anim #la variable d'animation doit etre dans le scope global pour ne pas disparaitre (cf documentation matplotlib)
    anim = animation.FuncAnimation(fig, animate,
                                frames=2000, interval=50, blit=True,
                                repeat=True) #animation de 2000 images a ~20 fps
bouton_start.on_clicked(starter)

plt.show()