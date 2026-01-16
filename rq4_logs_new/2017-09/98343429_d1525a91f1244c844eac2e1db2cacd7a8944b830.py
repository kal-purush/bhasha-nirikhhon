#!/usr/bin/env python

def check_guess(guessed_characters, secret_word):
    num_failed = 0
    print secret_word
    for c in secret_word:
        if c in guessed_characters:
            print c
        else:
            num_failed = num_failed + 1
            print '-'
    if num_failed > 0:
        return False
    else:
        return True

secret_word ='abandon'

num_guess = 10

# create an empty list which will be used to store the guessed characters
guessed characters = []
while num_guess > 0:
    guess = raw_input("Guess a character: ")
    # append the new guessed character to the guess list
    guessed_characters.append(guess)
    # check the characters guessed so far
    check_guess(guessed_characters, secret_word)
    if win:
        print "You win!"
        break
    num_guess = num_guess -1