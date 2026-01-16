from enum import Enum
import re

class Terminaux(Enum):
    d = 0
    a = 1
    attribut = 2
    couleur = 3
    ecu = 4
    figure = 5
    nombre = 6
    et = 7

class Symbole(Enum):
    d = 0
    a = 1
    att = 2
    c = 3
    e = 4
    f = 5
    n = 6
    et = 7
    fin = 8
    A = 9
    C = 10
    P = 11
    P2 = 12
    E = 13
    F = 14
    LC = 15
    N = 16

################

NB_REGLES = 14
REGEX = {
    Terminaux.d:        re.compile(r"^(de\s|d')"),
    Terminaux.a:        re.compile(r"^a(\sla|ux?)?\s"),
    Terminaux.attribut: re.compile(r"^(retrait)(\s|$)"),
    Terminaux.couleur:  re.compile(r"^(or|argent|azur|gueules|sinople|pourpre|sable)(\s|$)"),
    Terminaux.ecu:      re.compile(r"^(parti|coupe|tranche|taille|ecartele|gironne|tierce)(\s|$)"),
    Terminaux.figure:   re.compile(r"^(pal|fasce|bande|barre|croix|sautoir)(\s|$)"),
    Terminaux.nombre:   re.compile(r"^([0-9]+)([^0-9]|$)"),
    Terminaux.et:       re.compile(r"^(et)(\s|$)")    
}
REGEX_ESPACES = re.compile(r"^\s+")

################

class Mot:
    def __init__(self, symbole, contenu):
        self.symbole = [s for s in Symbole if s.value == symbole][0]
        self.contenu = contenu

    def __repr__(self):
        return "%s -> %s" % (self.symbole, self.contenu)

    def afficher(self):
        print(self.__repr__())

class Analyse:
    def __init__(self, chaine):
        self.motcourant = None
        self.chaine = chaine

    def getMotCourant(self):
        return self.motcourant

    def lireMot(self):
        # Enleve les espaces indesirables
        self.chaine = REGEX_ESPACES.sub("", self.chaine)
        print("\"%s\""%self.chaine)
        if len(self.chaine) == 0:
            self.motcourant = Mot(Symbole.fin.value, None)
            return self.motcourant

        for t in Terminaux:
            m = REGEX[t].match(self.chaine)
            if m != None: # Mot reconnu
                self.motcourant = Mot(t.value, m.group(1))
                self.chaine = self.chaine[len(m.group(1)):] # Supprime le mot reconnu
                return self.motcourant
        self.motcourant = None
        return None
