import random
from itertools import product
from random import shuffle

ranks = ['A', '2', '3', '4', 
         '5', '6', '7', '8', 
         '9', '10', 'J', 'Q', 'K'] 

suits = ["\u2663", "\u2665", 
         "\u2666", "\u2660"] 


class Card:
    def __init__(self, rank, suit):
        self.rank = rank
        self.suit = suit
        self.sign = rank + suit
        self.weight = 0
        if rank == '2':
            self.weight = 2
        elif rank == '3':
            self.weight = 3
        elif rank == '4':
            self.weight = 4
        elif rank == '5':
            self.weight = 5
        elif rank == '6':
            self.weight = 6
        elif rank == '7':
            self.weight = 7
        elif rank == '8':
            self.weight = 8
        elif rank == '9':
            self.weight = 9
        elif rank == 'T':
            self.weight = 10
        elif rank == 'J':
            self.weight = 11
        elif rank == 'Q':
            self.weight = 12
        elif rank == 'K':
            self.weight = 13
        elif rank == 'A':
            self.weight = 14
        else:
            print('Error')
    
        if suit == '1':
            self.suit = "\u2663"
        elif suit == '2':
            self.suit = "\u2665"
        elif suit == '3':
            self.suit = "\u2666"
        elif suit == '4':
            self.suit = "\u2660"
        else:
            print('Error')

    def show_deck():
        print("\n--- Whole deck of card ---\n")
        for rank in ranks: 
            for suit in suits: 
                print(f'{rank} of {suit}'.ljust(10), end='')
            print()


    def shuffle_deck():
        deck = list(product(ranks, suits)) 
        shuffle(deck)
        print("\n--- Shuffled deck ---\n")
        for i in range(0, len(deck), 4): 
            print("{} {} {} {}".format( 
                deck[i][0] + deck[i][1], 
                deck[i+1][0] + deck[i+1][1], 
                deck[i+2][0] + deck[i+2][1], 
                deck[i+3][0] + deck[i+3][1] 
            ))  

    show_deck() # Testuoti parodyti visas kortas
    shuffle_deck() # Testuoti parodyti visas ismaisytas kortas


class Deck:
    def __init__(self):
        print("Creating new deck")

    def play_war():
        print("--- WAR!!! ---")
        hand1 = ranks + suits
        hand2 = ranks + suits
        
        print (f'CPU: {hand1} vs You: {hand2}')
        if rank in hand1 > rank in hand2:
            print("You lose!")
        elif rank in hand1 < rank in hand2:
            print("You win!")
        elif rank in hand1 == rank in hand2:
            print("It's a Draw! Play again!")


  #  play_war() # Dar tvarkoma


    