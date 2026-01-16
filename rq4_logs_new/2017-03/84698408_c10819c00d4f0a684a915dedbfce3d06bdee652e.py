"""
classes:
graph thing (adj list)
tickets
players (+ownership)
    can have their own adjlists
"""
class Track:
    def __init__(self): #to be upd8ed
        self.owner = []
        self.maxOwners = None
        self.color = None
        self.length = 0

    def full(self):
        return len(self.owner) == self.maxOwners



class TrainGraph:
    #we like trains
    stations = dict() # the place where num : place is kept
    trans = {v:k for k,v in stations.items()} # reverse correspondence
    def __init__(self):
        self.adj = [[] for i in stations]


class CardSet:
    def __init__(self, red=0, blue=0, green=0, black=0, orange=0, wild=0): # i don't remember all the colors
        self.cards = [red, blue, green, black, orange, wild]
        self.trans = {'r':0, 'bu':1, 'g':2, 'bl':3, 'o':4, 'w':5}

    def addCard(self, num, color):
        self.cards[self.trans[color]] += num

    def canBuy(self, num, color):
        """
        Can you buy a thing given cost?
        """
        if color is None:
            for i in self.cards:
                if i >= num: return true
            return False
        else:
            color = self.trans[color]
            return self.cards[color] + self.cards[self.trans['w']] >= num