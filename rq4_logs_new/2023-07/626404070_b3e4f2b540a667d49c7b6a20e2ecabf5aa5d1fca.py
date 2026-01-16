from .Tipos import SimbolosArvore as SIMBOLO

class Nodo:
    def __init__(self, filho1, filho2, tipo, simbolo) -> None:
        self.tipo = tipo 
        self.filho1 = filho1
        self.filho2 = filho2
        self.tipo = tipo
        self.simbolo = simbolo
        self.indice = None
    
    # Os dois filhos são nulos
    def isFolha(self) -> bool:
        return self.filho1 is None and self.filho2 is None
    

    # Segue as regras de ser anulável
    def isAnulavel(self):
        if self.simb == '&':
            return True

        elif self.isFolha:
            return False

        elif self.tipo == SIMBOLO.CAT:
            return self.filho1.anulavel and self.filho2.anulavel

        elif self.tipo == SIMBOLO.OR:
            return self.filho1.anulavel or self.filho2.anulavel

        elif self.tipo == SIMBOLO.STAR:
            return True
    
    # Regras para first pos (filho1 como referência)
    def firstPos(self):
        if self.simbolo == '&':
            return set()

        elif self.isFolha:
            return set([self.indice])

        elif self.tipo == SIMBOLO.CAT:
            if self.filho1.isAnulavel:
                return self.filho1.firstpos.union(self.filho2.firstPos)
            
            else:
                return self.filho1.firstPos

        elif self.tipo == SIMBOLO.OR:
            return self.filho1.firstPos.union(self.filho2.firstPos)

        elif self.tipo == SIMBOLO.STAR:
            return self.filho1.firstPos
        
    # Segue as regras second pos (filho2 como referência)
    def secondPos(self):
        if self.simbolo == '&':
            return set()

        elif self.isFolha:
            return set([self.indice])

        elif self.tipo == SIMBOLO.CAT:
            if self.filho2.anulavel:
                return self.filho1.secondPos.union(self.filho2.secondPos)
            
            else:
                return self.filho2.secondPos

        elif self.tipo == SIMBOLO.OR:
            return self.filho1.secondPos.union(self.filho2.secondPos)

        elif self.tipo == self.STAR:
            return self.filho2.secondPos
        
    def criarIndice(self, i):
        if not self.isFolha and self.tipo != SIMBOLO.STAR:
            i = self.filho1.criarIndice(i)
            i = self.filho2.criarIndice(i)
        
        elif self.tipo == self.STAR:
            i = self.filho1.criarIndice(i)

        else:
            self.indice = i
            i += 1

        return i
    
    def pegarAlfabeto(self, alfabeto = None):
        if alfabeto is None:
            alfabeto = set()

        # Chegamos no final, podemos parar de adicionar os simbolos
        if self.isFolha:
            if self.simbolo != '#':
                alfabeto.add(self.simbolo)
        else:
            alfabeto = self.filho1.pegarAlfabeto(alfabeto)
            alfabeto = self.filho2.pegarAlfabeto(alfabeto)

        return alfabeto
    
    def pegarCorrespondentes(self, correspondentes = None):
        if correspondentes is None:
            correspondentes = dict()

        if self.isFolha:
            correspondentes[self.indice] = self.simbolos
        else:
            correspondentes = self.filho1.pegarCorrespondentes(correspondentes)
            correspondentes = self.filho2.pegarCorrespondentes(correspondentes)

        return correspondentes
    
    def __str__(self, level = 0):
        string = "  " * level
        if self.isFolha():
            string += f'{self.simbolo} {self.indice} \n'
        else:
            string += f'{self.tipo} \n'

        if self.tipo == SIMBOLO.STAR:
            string += self.filho1.__str__(level + 1)
        else:
            for filho in (self.filho2, self.filho1):
                if filho is not None:
                    string += filho.__str__(level + 1)
        return string