# -*- coding: utf-8 -*-

# Copyright (C) 2017 Lucas Robidou
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# PS: For any question or any suggestion, please contact me at:
#luc.robidou@gmail.com

# but du programme:
#determiner, par un algorithme genetique, le meilleur moyen de passer
#par n points points de passage

# ----- import des modules necessaires ---------------------------------
import matplotlib.pyplot as plt
import numpy as np
import random as rd
from itertools import permutations

# ----- definition des fonctions ---------------------------------------
def generer_villes(n, vmax) :
    """
        Return an array that contain list like [x,y] where x and y are 
        the coordinates of one city.
    
        :param a: Number of cities.
        :param b: The maximum each coordinate can take.
        :type a: integer
        :type b: integer
        :return: The coorinates of cities
        :rtype: array
    """
    villes = []
    while len(villes) < n :
        x = rd.randrange(0,vmax)
        y = rd.randrange(0,vmax)
        if not([x,y] in villes) :
            villes.append([x,y])
    return np.array(villes)

def ville_origine() :
    """
    but: generer une position initiale
    entree: rien
    sortie: tuple
    """
    return 0,0

def normaliser_chemin(chemin, n) :
    """
    but: creer un chemin valide de taille n
    entree: liste, entier
    sortie: liste
    """
    normal = []
    for elem in chemin :
        if 0 <= elem <n and elem not in normal :
            normal.append(elem)
    # on complete
    for k in range(n) :
        if k not in normal :
            normal.append(k)
    return normal

def distance(pts,i,j) :
    """
    but: caluler la distance entre deux points
    entree: liste, entier, entier
    sortie: entier
    """
    return np.sqrt( (pts[i][0]-pts[j][0])**2 + (pts[i][1]-pts[j][1])**2 )

def longueur_chemin(chemin, d) :
    """
    but: calculer la longueur d'un chemin
    entree: liste, liste de liste
    sortie: entier
    """
    n = len(chemin)
    s =matrice_distances[n][chemin[0]]
    for k in range(n-1) :
        s +=matrice_distances[chemin[k]][chemin[k+1]]
    return s

def calculer_distances(villes) :
    """
    but: calculer la matrice representant les distances de villes
    entree: liste
    sortie: liste de liste
    """
    n = len(villes)
    pts = [[villes[i][j] for j in range(2)] for i in range(n)]
    x,y = ville_origine()
    pts.append([x,y])
    return [[distance(pts,i,j) for j in range(n+1)] for i in range(n+1)]

def creer_population(m,d):
    population = []
    k = len(d)-1#nombre de points
    L = [i for i in range(k)]#liste des points
    for i in range(m):
        N = L[:]
        chemin = []
        while len(N) > 0:
            j = rd.randint(0,len(N)-1)
            chemin.append(N.pop(j))
        population.append((chemin, longueur_chemin(chemin,d)))
    return population

def reduire(p):
    ordre = []
    for e in p:
        ordre.append(e[1])
    arg = np.array(ordre).argsort()
    p_tri = p[:]
    #on "vide" p
    for i in range(len(p)):
        p.pop(0)
    for e in arg[:len(arg)//2]:
        p.append(p_tri[e])
        

def muter_chemin(c):
    i,j = rd.randint(0,len(c)-1), rd.randint(0,len(c)-1)
    c[i], c[j] = c[j], c[i]


def muter_population(p,proba,d):
    for i in range(5,len(p)):
        if rd.random()<proba:
            muter_chemin(p[i][0])
            p[i] = p[i][0],longueur_chemin(p[i][0],d)

def croiser(c1, c2):
    c = []
    for i in range(len(c1)//2):
        c.append(c1[i])
    for i in range(len(c2)//2,len(c2)):
        c.append(c2[i])
    c = normaliser_chemin(c,n)
    return c

def nouvelle_generation(p,d):
    for i in range(-1,len(p)):
        croisement = croiser(p[i][0],p[i+1][0])
        p.append((croisement,longueur_chemin(croisement,d)))

def algo_genetique(villes,m,proba,g):
    pop = creer_population(m,matrice_distances)
    plt.ion()
    generation = 0
    l = []
    for e in pop:
        l.append(e[1])
    l.sort()
    plt.ylabel('lengh of paths')
    plt.xlabel('<--- faster              paths            longer ----->')
    plt.title("Travelling salesman genetic")
    ax = plt.gca()
    wframe = ax.plot(l,c = 'b')
    for i in range(g):
        generation += 1
        
        #équipe bleue
        reduire(pop)
        nouvelle_generation(pop,matrice_distances)
        muter_population(pop,0.3,matrice_distances)
        
        l = []
        for e in pop:
            l.append(e[1])
        ax = plt.gca()
        l.sort()
        wframe = ax.plot(l,c = 'b')
        
        plt.draw()
        plt.pause(0.001)
        liste = wframe.pop(0)
        liste.remove()
        del liste
    return l



# ----- corps du programme principal -----------------------------------

n = 32
villes = generer_villes(n,25)
matrice_distances = calculer_distances(villes)
pop = creer_population(20, matrice_distances)

t = algo_genetique(villes,500,1,10000)