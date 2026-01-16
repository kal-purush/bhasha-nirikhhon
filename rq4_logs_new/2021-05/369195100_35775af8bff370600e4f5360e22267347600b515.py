############
# Exercice 1
############

# Créer une fonction "power", qui prend en paramètre une chiffre et une puissance, et qui retourne le chiffre élevé à la puissance données.
# Exemples :
# power(2, 2) renvoie 4
# power(2, 3) renvoie 8
# power(3, 9) renvoie 19683
# power(2, -1) renvoie 0.5


############
# Exercice 2
############

# Ecrire une fonction "helloGuys", qui prend en paramètre une liste de prénoms, et qui affiche à l'écran "Hello X" pour chaque membre de la liste.

############
# Exercice 3
############

# Ecrire une fonction "operation" qui prend en paramètre un premier chiffre, un second chiffre, puis un mot parmis les mots : add, sub, mult, div ou div-entiere
# La fonction doit retourner le résultat de l'opération demandée entre les deux chiffres.
# La fonction doit gérer les différents cas d'erreurs définis dans la Leçon 1 / Exercice 2
# Tester les cas suivants :
# operation(1, 2, "add") doit renvoyer 3
# operation(1, 2, "sub") doit renvoyer -1
# operation(2, 3, "mult") doit renvoyer 6
# operation(3, 2, "div") doit renvoyer 1.5
# operation(3, 2, "div-entiere") doit renvoyer 1
# operation(3, 0, "div") doit afficher "Impossible de diviser par zéro", et retourner None
# operation(3, 2, "toto") doit afficher "Cette opération n'est pas reconnue", et retourner None
# operation(1, 0.5, "add") doit renvoyer 1.5

#############################################################################################
# Exercice 4 bis ! Bonus !
# Exercice difficile, d'autant plus que tu auras besoin d'informations pas vues dans le cours
#############################################################################################

# Ecrire une fonction "prettySum", capable de prendre en paramètre n'importe quel nombre de chiffres (entre 0 et l'infini)
# La fonction doit renvoyer la somme de tous ces nombres
# Exemples :
# prettySum(1, 2) doit renvoyer 3
# prettySum(4, 5, 6, 7) doit renvoyer 22
# prettySum() doit renvoyer 0

#######################################
# Exercice 4 ter ! Bonud ! Pas simple !
#######################################

# J'en ai un peu marre de rajouter des "#" dans les énoncés de chaque exercice, et de faire en sorte qu'il y ait le bon nombre de dièses pour s'aligner sur le texte.
# Je te demande de m'écrire une fonction "getPrettyPrint":
# - qui prend en paramètre une chaine de charactère (minimum 3 charactère - pas besoin de vérifier cette condition)
# - qui retourne une liste contenant 3 chaines de charactères contenant le message "formaté" avec des dièses, de la même manière que j'ai écris l'intitulé de chaque exercice

# Exemples :
# getPrettyPrint("Exo 1") doit renvoyer ["#######", "# Exo 1", "#######"]
# getPrettyPrint("Dernier Exo") doit renvoyer ["#############", "# Exo 1", "#############"]

# Le code suivant doit pouvoir s'executer et afficher à l'écran chaque titre d'exercice formaté :
# print("\n".join(getPrettyPrint("Exo 1")))
# print("\n".join(getPrettyPrint("Dernier Exo")))
# print("\n".join(getPrettyPrint("teeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeest !")))