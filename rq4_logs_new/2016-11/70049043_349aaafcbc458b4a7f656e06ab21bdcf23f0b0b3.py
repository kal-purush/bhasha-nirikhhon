# Mini-project #6 - Blackjack

import simplegui
import random

# load card sprite - 936x384 - source: jfitz.com
CARD_SIZE = (72, 96)
CARD_CENTER = (36, 48)
card_images = simplegui.load_image("http://storage.googleapis.com/codeskulptor-assets/cards_jfitz.png")

CARD_BACK_SIZE = (72, 96)
CARD_BACK_CENTER = (36, 48)
card_back = simplegui.load_image("http://storage.googleapis.com/codeskulptor-assets/card_jfitz_back.png")    

# initialize some useful global variables
in_play = False
outcome = ""
dealer_outcome = ""
score = 0

# define globals for cards
SUITS = ('C', 'S', 'H', 'D')
RANKS = ('A', '2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K')
VALUES = {'A':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9, 'T':10, 'J':10, 'Q':10, 'K':10}


# define card class
class Card:
    def __init__(self, suit, rank):
        if (suit in SUITS) and (rank in RANKS):
            self.suit = suit
            self.rank = rank
        else:
            self.suit = None
            self.rank = None
            print "Invalid card: ", suit, rank

    def __str__(self):
        return self.suit + self.rank

    def get_suit(self):
        return self.suit

    def get_rank(self):
        return self.rank

    def draw(self, canvas, pos):
        card_loc = (CARD_CENTER[0] + CARD_SIZE[0] * RANKS.index(self.rank), 
                    CARD_CENTER[1] + CARD_SIZE[1] * SUITS.index(self.suit))
        canvas.draw_image(card_images, card_loc, CARD_SIZE, [pos[0] + CARD_CENTER[0], pos[1] + CARD_CENTER[1]], CARD_SIZE)
        
# define hand class
class Hand:
    def __init__(self):
        self.hand_cards = []

    def __str__(self):
        s = ""
        for i,c in enumerate(self.hand_cards):
           s += "Card" + str(i+1) + ": "
           s += c.get_suit()
           s += c.get_rank()
           s += "\n"
        
        return s
    def add_card(self, card):
        self.hand_cards.append(card)

    def get_value(self):
        # count aces as 1, if the hand has an ace, then add 10 to hand value if it doesn't bust
        total = 0
        for c in self.hand_cards:
            total += VALUES[c.get_rank()]
        
        return total
   
    def draw(self, canvas, pos):
        # draw a hand on the canvas, use the draw method for cards
        for i,c in enumerate(self.hand_cards):
            c.draw(canvas, [CARD_SIZE[0]*(i+1),pos[1]])
        
# define deck class 
class Deck:
    def __init__(self):
        self.deck_cards = []
        for suit in SUITS:
            for rank in RANKS:
                card = Card(suit,rank)
                self.deck_cards.append(card)

    def shuffle(self):
        random.shuffle(self.deck_cards)

    def deal_card(self):
        return self.deck_cards.pop()
    
    def __str__(self):
        s = ""
        for card in self.deck_cards:
            s += card.get_suit()+card.get_rank()
            s += "\n"
      
        return s



#define event handlers for buttons
def deal():
    global outcome, in_play, player, dealer, deck, score
    if in_play:
        score -= 1
        outcome = "You choose to lose!"
    deck = Deck()
    deck.shuffle()
    player = Hand()
    dealer = Hand()
    #each player has two cards in the beginning
    player.add_card(deck.deal_card())
    player.add_card(deck.deal_card())
    dealer.add_card(deck.deal_card())
    dealer.add_card(deck.deal_card())
    
    outcome = "Hit or stand?"
    
    in_play = True

def hit():
    # if the hand is in play, hit the player
    global outcome, player, in_play, dealer_outcome, score
    if in_play:
        player.add_card(deck.deal_card())
        # if busted, assign a message to outcome, update in_play and score
        if player.get_value() > 21:
            outcome = "You have busted!"
            score -= 1
            dealer_outcome = "You win!"
            in_play = False
            print outcome
        print player
        print player.get_value()   
def stand():
    global score, outcome, player, dealer, dealer_outcome, in_play
    if in_play:
        while (dealer.get_value() < 17):
            dealer.add_card(deck.deal_card())
            print dealer, dealer.get_value()
        if dealer.get_value() > 21:
            dealer_outcome = "You went bust and lose!"
            score += 1
        else:
            if dealer.get_value() > player.get_value():
                dealer_outcome = "You win!"
                outcome = "You lose!"
                score -= 1
            elif player.get_value() > dealer.get_value():
                dealer_outcome = "You lose!"
                outcome = "You win!"
                score +=1
            else:
                dealer_outcome = "Draw!"
                outcome = "Draw!"
            print outcome
        in_play = False
    # if hand is in play, repeatedly hit dealer until his hand has value 17 or more

    # assign a message to outcome, update in_play and score

# draw handler    
def draw(canvas):
    # test to make sure that card.draw works, replace with your code below
    
    #card = Card("S", "A")
    #card.draw(canvas, [300, 300])
    canvas.draw_text("BlackJack", [250, 40], 30,"Black")
    canvas.draw_text("Score  " + str(score), [450, 40], 20,"Black")
    canvas.draw_text("Dealer", [80, 150], 20,"Black")
    canvas.draw_text(dealer_outcome, [200, 150], 20,"Black")
    dealer.draw(canvas, [300,200])
    
    canvas.draw_text("Player", [80, 350], 20,"Black")
    canvas.draw_text(outcome, [200, 350], 20,"Black")
    player.draw(canvas, [300,400])
    
    if in_play:
        canvas.draw_polygon([(145, 200), (145, 200+CARD_SIZE[1]), (145+CARD_SIZE[0], 200+CARD_SIZE[1]), (145+CARD_SIZE[0], 200)], 1, 'black', 'black')



# initialization frame
frame = simplegui.create_frame("Blackjack", 600, 600)
frame.set_canvas_background("Green")


#create buttons and canvas callback
frame.add_button("Deal", deal, 200)
frame.add_button("Hit",  hit, 200)
frame.add_button("Stand", stand, 200)
frame.set_draw_handler(draw)


# get things rolling
deal()
frame.start()


# remember to review the gradic rubric