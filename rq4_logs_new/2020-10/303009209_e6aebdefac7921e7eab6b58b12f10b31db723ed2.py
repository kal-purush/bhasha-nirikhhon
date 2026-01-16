from enum import Enum
from enum import IntEnum
from random import *

fullDeck = []
partialDeck = []
player1cards = []
player2cards = []
dealer = bool

# # Card enum for cards
class Card(IntEnum):

    ACE = 1
    TWO = 2
    THREE = 3
    FOUR = 4
    FIVE = 5
    SIX = 6
    SEVEN = 7
    EIGHT = 8
    NINE = 9
    TEN = 10
    JACK = 11
    QUEEN = 12
    KING = 13


# # Suit Enum for cards
class Suit(Enum):
    SPADES = 'spades'
    CLUBS = 'clubs'
    HEARTS = 'hearts'
    DIAMONDS = 'diamonds'


# # Class to hold card info
class PlayingCard:
    def __init__(self, card_name, card_numb, card_suit, card_value,discard=False):
        self.name = card_name
        self.numb = card_numb
        self.suit = card_suit
        self.value = card_value
        self.discard = discard

    def textPlay(self):
        output = self.name.name + " " + self.suit.name
        return output


def createDeck():
    for suit in Suit:
        for card in Card:
            if card.value > 10:
                cardValue = 10
            else:
                cardValue = card.value
            fullDeck.append(PlayingCard(card, card.value, suit, cardValue))
    shuffle(fullDeck)


# # Draw single card from deck
def cutDeck(deck):
    input("Hit enter to cut the deck")
    rand_card = randint(0, len(deck)-1)
    return deck.pop(rand_card)


def cutToStart():

    winnerFound = False
    while not winnerFound:

        createDeck()
        input("Cut and see who goes first")
        cutDepth = randint(10, 40)
        p1cut = fullDeck[cutDepth-1]
        p2cut = cutDeck(fullDeck[cutDepth + 1:])
        if p1cut.numb < p2cut.numb:
            dealer = True
            winnerFound = True
            print("Player 1 cut - ", p1cut.textPlay())
            print("Computer cut - ", p2cut.textPlay())
            input("You win!! Your turn to deal")
        elif p1cut.numb > p2cut.numb:
            dealer = False
            winnerFound = True
            print("Player 1 cut - ", p1cut.textPlay())
            print("Computer cut - ", p2cut.textPlay())
            input("You loose! Computer goes first")
        else:
            print("Player 1 cut - ", p1cut.textPlay())
            print("Computer cut - ", p2cut.textPlay())
            input("TIE! Draw again")
            winnerFound = False
    return dealer


def deal():
    partialDeck = list(fullDeck)
    if dealer:
        for i in range(0, 6):
            player1cards.append(partialDeck.pop())
            player2cards.append(partialDeck.pop())


# dealer =  cutToStart()

createDeck()

dealer = True

deal()


























