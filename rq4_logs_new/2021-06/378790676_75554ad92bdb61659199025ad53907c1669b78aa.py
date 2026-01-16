import random

class Baralho():

    def __init__(self):
        self.baralho = [['Fogo', 1], ['Fogo', 2], ['Fogo', 3], ['Fogo', 4], ['Fogo', 5], ['Agua', 1], ['Agua', 2], ['Agua', 3],['Agua', 4],
        ['Agua', 5],['Ar', 1],['Ar', 2],['Ar', 3],['Ar', 4],['Ar', 5],['Terra', 1],['Terra', 2],['Terra', 3],['Terra', 4],['Terra', 5]]

    def embaralhar(self):
        count = 1;
        verificar = True
        cartasDoJogo = []

        while count != 11:
            while verificar:
                try:
                    numCarta = random.randint(0,19)
                    teste = self.baralho[numCarta]
                    verificar = False
                except:
                    verificar = True

            verificar = True
            count = count + 1

            cartasDoJogo.append(self.baralho[numCarta])
            del(self.baralho[numCarta])

        return cartasDoJogo