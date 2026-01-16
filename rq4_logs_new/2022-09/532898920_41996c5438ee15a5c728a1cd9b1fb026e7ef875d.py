class LowerAttrMeta(type):
    def __new__(cls, clsname, bases, dct):
        lowercase_attr={}
        for name, val in dct.items():
            if not name.startswith('__'):
                lowercase_attr[name.lower()] = val
            lowercase_attr[name] = val
        bases= (Manouvellebase,)
        return type.__new__(cls, clsname, bases, lowercase_attr)

class Manouvellebase:
    def mafonctionuniverselle(self):
        return "Un méthodes implémentée par toute mes classes de métaclass LowerAttrMeta car elles sont toutes des sous classe de la super classes Manouvellebase."

class Maclassacorriger(metaclass=LowerAttrMeta):
    def __init__(self,second=2):
        self.compteur=1
        self.second=second

    
    def GET_COmpteur(self):
        return self.compteur

    @property
    def second(self):
        return self._second

    @second.setter
    def second(self, value):
        self._second=value
        self.compteur+=1