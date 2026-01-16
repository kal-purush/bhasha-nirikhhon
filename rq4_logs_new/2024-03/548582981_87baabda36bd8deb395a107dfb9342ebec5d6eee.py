class Aflitos:
    def __init__(self, n, setores):
        self.n = n
        self.setores = setores
        self.matrizSet = [-1] * n

    def somaTorcedores(self):
        return self.soma(self.n-1)

    def soma(self, setNum):
        if setNum < 0:   #evitar run time error
            return 0
        elif setNum == 0:   # um termo
            return self.setores[0]
        elif setNum == 1:    # maximo entre os dois primeiros termos
            return max(self.setores[0], self.setores[1])

        if self.matrizSet[setNum] != -1:   # Verifica se já foi calculada
            return self.matrizSet[setNum]

        comp1 = self.soma(setNum-2) + self.setores[setNum]
        comp2 = self.soma(setNum-1)
        self.matrizSet[setNum] = max(comp1, comp2)  # armazena
        return self.matrizSet[setNum]

# Parte principal
n = int(input())
setores = list(map(int, input().split()))

# Soma pela classe
foto = Aflitos(n, setores)
somaSet = foto.somaTorcedores()

# output
print(f'{somaSet} torcedores podem ser fotografados.')

