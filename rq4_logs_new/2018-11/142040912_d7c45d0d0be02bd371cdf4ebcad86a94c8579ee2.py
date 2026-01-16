
class Pentagono:
    def __init__(self, lado, apotema):
        self.lado = lado
        self.apotema = apotema
    @property
    def area(self):
        areap = (self.perimetro * self.apotema) / 2
        return areap
    @property
    def perimetro(self):
        perimp = self.lad o *5
        return perimp

    def __str__(self):




a = Pentagono(5, 3)


print(a.area)