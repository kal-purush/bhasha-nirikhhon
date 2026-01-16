import numpy as np
import pandas as pd
from functools import reduce
import operator

dataframe = pd.DataFrame([["drone", 0, 1, 2],
                         ["poupé", 1, 0, 1],
                         ["voiture télécomandé", 0, 1, 2],
                         ["playstation", 0, 0, 1],
                         ["xbox", 1, 1, 2],
                         ["album de musique kpop", 1, 0, 2],
                         ["ablum de musique rap", 0, 0, 1],
                         ["bracelet", 1, 0, 2],
                         ["rouge à levre", 1, 0, 2],
                         ["crampons de foot", 0, 1, 2],
                         ["cliché", 2, 1, 1],
                         ["plus d'idee", 2, 0, 2]],
                         ##Question 1 : Es une fille ?
                         columns=['objet', 'reponse1', 'reponse2', 'reponse3'])
                         ##columns=['prix', 'surface', 'garage', 'nb_piece']

##### regle binaire : 0=non // 1=oui // 2=je ne sais pas (donc = 0 et 1)

#print(dataframe)

class decision_tree_regressor :
  """ Cette classe a pour but de créer un algorithme d'apprentissage automatique
  d'arbres de décision regresseur"""

  def __init__(self, target, dataframe, max_depth):
    """Cette fonction a pour but d'initialiser les variables essentiel à la 
    construction de notre algorithme.
   INPUT 
    - target : la variable cible qu'il faudra classifier
    - dataframe : les données d'apprentissage
    - max_deapth : la profondeur maximal de l'arbre à entraîner
   """
    self.max_depth = max_depth
    self.target = target
    self.dataframe = dataframe

  def __quanti_split__(self, feature, value, dataset):
   """Cette fonction split un jeu de données en fonction
   de la valeur 'value' de la variable quantitative 'feature' passé en paramètre
   INPUT 
    - feature : integer correspondant à la variable à séparer
    - value : integer correspond à la valeur à laquelle séparer notre jeu de données
    - dataset : pandas dataframe à séparer
   OUTPUT 
    - left : dataframe avec les données où 'feature' est plus petit ou égale à 'value'
    - right : dataframe avec les données où 'feature' est plus grande que 'value' 
   """
   
   left = dataset[dataset.loc[:,feature]<= value]
   right = dataset[dataset.loc[:,feature]> value]

   return left, right

  def __quali_split__(self, feature, value, dataset):
   """Cette fonction split un jeu de données en fonction
   de la valeur 'value' de la variable qualitative'feature' passé en paramètre
   INPUT 
    - feature : integer correspondant à la variable à séparer
    - value : integer correspond à la valeur à laquelle séparer notre jeu de données
    - dataset : pandas dataframe à séparer
   OUTPUT 
    - left : dataframe avec les données où 'feature' est égale 'value'
    - right : dataframe avec les données où 'feature' est différent 'value'
   """
   
   left = dataset[dataset.loc[:,feature]== value]
   right = dataset[dataset.loc[:,feature]!= value]

   return left, right

tree = decision_tree_regressor('objet', dataframe, 4)
left_node, right_node = tree.__quanti_split__('reponse1', 0, dataframe)
left_node2,right_node2 = tree.__quanti_split__('reponse2', 0, dataframe)

###### Possibilité de la question 1 : Es une fille ?
print("-----------------Objet pour les Hommes--------------")
print(left_node)
print("-----------------Objet pour les femmes--------------")
print(right_node)
print("-----------------Objet pour les femmes de -12ans--------------")
print(right_node2)
