import random

ranks = [2, 3, 4, 5, 6, 7, 8, 9, 10, 'J', 'K', 'A', 'Q']
suits = ['Hearts', 'Diamonds', 'Spades', 'Clubs']

class Card():
    def __init__(self, suit, rank):
        self.suit = suit
        self.rank = rank

    def convert_number_into_symbol(self):
        if self.suit == 'Hearts':
            return "\u2665"
        elif self.suit == 'Diamonds':
            return "\u2666"
        elif self.suit == 'Spades':
            return "\u2660"
        elif self.suit == 'Clubs':
            return "\u2663"

    def compare_card_rank(self, computer, player):
        print(f"Computer: {computer.rank} {computer.convert_number_into_symbol()}")
        print(f"Player: {player.rank} {player.convert_number_into_symbol()}")

        if computer.rank > player.rank:
            print("You lost")
        elif player.rank > computer.rank:
            print("You won")
        elif player.rank == computer.rank:
            print("Draw, flip again")

def create_deck():
    ranks = list(range(2, 15))
    deck = [Card(suit, rank) for suit in suits for rank in ranks]
    random.shuffle(deck)
    return deck

while True:
    deck = create_deck()

    player_card = deck.pop()
    computer_card = deck.pop()

    compare = Card('', '')
    compare.compare_card_rank(computer_card, player_card)

    play_again = input("Do you want to play again? (yes/no): ").lower()
    if play_again != 'yes':
        break