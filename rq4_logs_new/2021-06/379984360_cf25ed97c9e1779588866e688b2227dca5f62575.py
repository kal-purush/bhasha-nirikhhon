    def recuperer_item(liste, indice):
        if indice < 0 and abs(indice) <= len(liste):
            return liste[indice]
        elif indice > 0 and indice < len(liste):
    	return liste[indice]
        elif indice == 0 and liste:
            return liste[0]
     
        return "Index {} hors de la liste".format(indice)
     
    liste = ["Julien", "Marie", "Pierre"]
     
    print(recuperer_item(liste, 0))
    print(recuperer_item(liste, 5))
    print(recuperer_item(liste, -13))