class BankaHesabi:
    def __init__(self):
        self.bakiye = 0

    def para_cek (self,tutar):
        self.bakiye = self.bakiye - tutar
        return self.bakiye

    def para_yatir(self,tutar):
        self.bakiye = self.bakiye + tutar
        return self.bakiye

    def bakiye_yazdir(self):
        print(self.bakiye)

    def __str__(self):
        return str(self.bakiye)

    def __repr__(self):
        return str(self.bakiye)