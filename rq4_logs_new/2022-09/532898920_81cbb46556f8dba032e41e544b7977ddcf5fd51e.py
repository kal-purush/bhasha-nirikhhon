class Compteur :
    def __init__(self,pas = 1,valeur = 0) :
        self._valeur=valeur
        self._pas=pas

    def __str__(self) :
        return "Compteur( pas = " + str(self._pas) + ", valeur = " + str(self._valeur) + ")"
    def __lt__(self,other) :
        return self._valeur<other._valeur
    def __gt__(self,other) :
        return self._valeur>other._valeur
    def __eq__(self,other) :
        return self._valeur==other._valeur

    def ajoute(self) :
        self._valeur = self._valeur+self._pas
    def donne_valeur(self) :
        return self._valeur