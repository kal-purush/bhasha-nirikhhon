import random

class Card(object):
    def __init__(self, suit, rank):
        self.suit = suit
        self.rank = rank

    def __repr__(self):
        return "<Card: the {} of {}s>".format(self.rank, self.suit)


class Deck(object):

    def __init__(self):
        self.cards = []
        for suit in ["spade", "club", "heart", "diamond"]:
            for rank in ["ace", "2", "3", "4", "5", "6", "7", "8", "9", "10", "jack", "queen", "king"]:
                self.cards.append(Card(suit, rank))

        self.shuffle()

    def shuffle(self):
        random.shuffle(self.cards)

    def deal(self):
        return self.cards.pop()
