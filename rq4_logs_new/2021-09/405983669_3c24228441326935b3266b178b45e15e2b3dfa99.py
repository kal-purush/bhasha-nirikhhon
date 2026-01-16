"""
Les try et except sont là pour facilité l'exposition des erreurs à l'utilisateur
Lorsqu'il y a une erreurs quelconque dans le code, il sera possible de le renvoyer facilement l'utilisateur
"""

try: # Le bloc ci-dessous est un essaie pour de potentiel erreurs
    int(input("insérez un nombre : ")) # demande d'insérer un nombre et essaye de le convertir en valeur entier
except: # Si vous avez rentré des caractères dans l'input précédent, le bloc ci-dessous ce lancera
    print("Erreur, vous deviez rentrer un nombre") # Le texte ci-contre s'affiche lorsque vous entrez une chaîne de caractères
else: # Si tout c'est bien passé, le bloc suivant se lanera
    print("Tout est bon") # le texte ci-contre ce lancera donc si la valeur demandé est belle et bien une valeur entier

# autre exemple
try: # ici on va essayer de faire une division :
    d = int(input("Entrez un nombre qui divisera 10 - "))
    print(10/d)
except ZeroDivisionError: # Si l'utilisateur essaye de diviser par zero :
    print("Vous ne pouvez pas diviser par zero !")
except ValueError: # Si l'utilisateur essaye de mettre une chaîne de caractères
    print("Vous devez rentrer un nombre !")
except: # Une quelconque autre erreur
    print("Error")
else: # Si il n'y a aucune erreurs
    print("Aucun problème :)")
finally: # Le bloc ci-dessous ce lancera dans tout les cas
    print("Fin programme")

"""
Il existe plusieurs type d'erreurs
    - ZeroDivisionError : division par zero
    - ValueError : mauvaise affectation des valeur
    - AssertionError : erreur d'assertion
    - TypeError : pour les type de variable
    - NameError : variable non définie
    - OSError : erreur systeme
    - etc (il y en a encore plein d'autre)
"""

# Raise :

inventory = input("Combien d'item voulez vous ajouter ? - ")
try:
    inventory = int(inventory)
except:
    print("La valeur doit être un nobmre !")
else:
    if(inventory>5):
        raise ZeroDivisionError("Inventory full !")
    else:
        print("Vous pouvez ajouter vos items")
# Asertion :
