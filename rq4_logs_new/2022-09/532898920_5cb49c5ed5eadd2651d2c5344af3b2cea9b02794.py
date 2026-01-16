from dice import Dice

class Tapis_vert:
    def __init__(self) :
        self.des=tuple(Dice() for x in range(0,5))
        self.full=0
        self.paire=0
        self.double_paire=0
        self.carre=0
        self.brelan=0
    def __getitem__(self,number):
        return self.des[number]
    
    def __str__(self):
            return str([str(self.des[x]) for x in range(0,5)]) 



    def main(self,printe = 0):
        dict={x:0 for x in range(1,7)}
        self.full=0
        for i in self.des:
            dict[i.position]=dict[i.position]+1
   
        self.paire = list(dict.values()).count(2)
        self.double_paire = self.paire//2
        self.brelan = list(dict.values()).count(3)
        self.carre = list(dict.values()).count(4)
        self.full = ((self.paire-self.double_paire)+self.brelan)//2
        self.paire = self.paire-self.full-(2*self.double_paire)
        self.brelan = self.brelan-self.full
        if printe==1 :
            print("Paire = " + str(self.paire), "Brelan = " + str(self.brelan), "Double Paire = " + str(self.double_paire), "Carre = " + str(self.carre), "Full = " + str(self.full))
        return [self.paire,self.brelan,self.double_paire,self.carre,self.full]
    
    def roll(self,printe=0):
        for i in range(0,5):
            self.des[i].roll()
        self.main(printe)
        return self