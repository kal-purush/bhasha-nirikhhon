#Un petit Test sur les modules de fonctions
print("Exercice : Utilisation du module Turtle pour dessiner des carres")

from turtle import *

def carre(taille, couleur):
    "Fonction qui dessine un carré de Taille t de Couleur bien déterminées !" #C'est comme ça qu'on fait la documentation d'une Fonction
    color(couleur) #Definir la couleur du carré
    c = 0 #Compteu pour le nombre de cotés
    while(c<4):
        forward(taille) #Avancer d'une distance = taille
        right(90) #Tourner à droite d'un angle de 90 degrés
        c = c + 1