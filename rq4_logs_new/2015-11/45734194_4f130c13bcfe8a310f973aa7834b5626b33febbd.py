from operator import itemgetter, attrgetter                                                           

class NumberClass:
    def __init__(self):
        #Turn into dictionary of the form (word category, list of words within that category
        self.probHash = []
        self.probPlus = []
        self.probBlank = []
        self.count = 0
        for i in range(784):
            self.probHash.append(0)
            self.probPlus.append(0)
            self.probBlank.append(0)
            
        
    def addToProbHash(self,adder, positiony, positionx):
        self.probHash[positionx+positiony*28] += adder
        
    def addToProbPlus(self,adder, positiony, positionx):
        self.probPlus[positionx+positiony*28] += adder
        
    def addToProbBlank(self,adder, positiony, positionx):
        self.probBlank[positionx+positiony*28] += adder
        
    def addToCount(self, adder):
        self.count += adder
    
    def getProbHash (self, positiony, positionx):
        HashValue = self.probHash[positionx+28*positiony]
        return HashValue
    
    def getProbPlus (self, positiony, positionx):
        PlusValue = self.probPlus[positionx+28*positiony]
        return PlusValue
    
    def getProbBlank (self, positiony, positionx):
        BlankValue = self.probPlus[positionx+28*positiony]
        return BlankValue

    def getCount(self):
        countValue = self.count
        return countValue