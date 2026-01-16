class Card:
    def __init__(self, suit, value):
        self.suit = suit
        self.value = value

class Player:
    def __init__(self, name):
        self.name = name
        self.hand = []

class Game:
    def __init__(self):
        self.deck = []
        self.players = []
        self.trump = None

    def create_deck(self):
        suits = ['hearts', 'diamonds', 'clubs', 'spades']
        values = ['6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
        for suit in suits:
            for value in values:
                self.deck.append(Card(suit, value))

    def shuffle_deck(self):
        import random
        random.shuffle(self.deck)

    def draw_card(self):
        return self.deck.pop()

    def start_game(self):
        self.create_deck()
        self.shuffle_deck()
        self.trump = self.draw_card()

    def add_player(self, name):
        if len(self.players) < 6:
            self.players.append(Player(name))
        else:
            print("Cannot add more players, the game is full.")

    def deal_cards(self):
        for player in self.players:
            for i in range(6):
                if self.deck:
                    player.hand.append(self.draw_card())
            
# Usage:

game = Game()
game.start_game()

# Add players
game.add_player("Player1")
game.add_player("Player2")

# Deal initial cards
game.deal_cards()
