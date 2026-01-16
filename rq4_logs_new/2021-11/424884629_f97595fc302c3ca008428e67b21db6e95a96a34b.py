#! /usr/bin/env python3

from random import shuffle

# INITIAL LIST

mylist = [" ", "O", " "]

# FUNCTIONS

def shuffle_list(myList):
    '''
    This function shuffles mylist, so that the
    index of "O" is always different.
    '''
    shuffle(myList)
    return myList

def player_guess():
    '''
    This function lets the user guess the index
    of "O" and stores their guess.
    '''
    guess = ""
    
    while guess not in ["0", "1", "2"]:
        guess = input("Pick a number between 0 and 2: ")
    
    return int(guess)

def check_guess(mylist, guess):
    '''
    This function compares the index of "O"
    with the user's guess, showing the result.
    '''
    if mylist[guess] == "O":
        print("Correct!")
    else:
        print("Wrong guess!")
        print(mylist)

# SHUFFLE LIST

mixedup_list = shuffle_list(mylist)

# USER GUESS

guess = player_guess()

# CHECK GUESS
# Here two functions will interact with each other
# within another function

check_guess(mixedup_list, guess)