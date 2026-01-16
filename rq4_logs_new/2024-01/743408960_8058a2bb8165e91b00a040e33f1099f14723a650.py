import random
from itertools import product
from time import sleep

ranks = [2, 3, 4, 5, 6, 7, 8, 9, 10, 'J', 'K', 'A', 'Q']
suits = ['Hearts', 'Diamonds', 'Spades', 'Clubs']

class Card():
    def __init__(self, suit, rank):
        self.suit = suit
        self.rank = rank
        self.weight = self.calculate_weight()

    def calculate_weight(self):
        if isinstance(self.rank, int):
            return self.rank
        elif self.rank == 'J':
            return 11
        elif self.rank == 'K':
            return 13
        elif self.rank == 'A':
            return 14
        else:
            return 12

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
        print(f"Computer: {computer.rank} {computer.convert_number_into_symbol()} Weight: {computer.weight}")
        print(f"Player: {player.rank} {player.convert_number_into_symbol()} Weight: {player.weight}")

        if computer.weight > player.weight:
            print("You lost")
        elif player.weight > computer.weight:
            print("You won")
        elif player.weight == computer.weight:
            print("Draw, flip again")

def create_deck():
    ranks = list(range(2, 15))
    deck = [Card(suit, rank) for suit in suits for rank in ranks]
    random.shuffle(deck)
    return deck

def take_from_top(deck):
    return deck.pop(0)

def take_from_bottom(deck):
    return deck.pop()

def take_random(deck):
    return deck.pop(random.randint(0, len(deck) - 1))

def show_deck(deck):
    print("\n--- Whole deck of cards ---\n")
    for card in deck:
        print(f'{card.rank} of {card.suit}'.ljust(20), end='')
    print()

def shuffle_deck(deck):
    print("\n--- Shuffled deck ---\n")
    random.shuffle(deck)
    for card in deck:
        print(f'{card.rank} of {card.suit}'.ljust(20), end='')
    print()

print("Welcome to the best card game!")

while True:
    deck = create_deck()

    show_deck(deck)
    shuffle_deck(deck)
    sleep(2)  # Adding a delay for better visibility

    choice = input("Do you want to take a card from the top or bottom? (top/bottom): ").lower()

    if choice == 'top':
        player_card = take_from_top(deck)
        computer_card = take_random(deck)
    elif choice == 'bottom':
        player_card = take_from_bottom(deck)
        computer_card = take_random(deck)
    else:
        print("Invalid choice. Please enter 'top' or 'bottom'.")
        continue

    compare = Card('', '')
    compare.compare_card_rank(computer_card, player_card)

    play_again = input("Do you want to play again? (yes/no): ").lower()
    if play_again != 'yes':
        break