import os
from puzzle import *

def clearScreen():
    system_platform = platform.system()
    if system_platform == "Windows":
        os.system("cls")  # Clear screen on Windows
    else:
        os.system("clear")  # Clear screen on macOS and Linux

def entryDisplay():
    print('--------------------------------')
    print('Welcome to Spelling Bee!')
    print('Created by Big Python Energy')
    print('--------------------------------')

def mainGameDisplay(puzzle):
    print('LETTERS:')
    print('-----------------')
    print(puzzle.getAllLetters() + "\n")
    print('Must Contain: ' + puzzle.specialLetter)
    print('-----------------\n')
    print('Score:')
    print(puzzle.getCurrentScore())
    print ('\nEnter your guess below!\n\nYou may also:\nEnter /words for a list of words, \nEnter /shuffle to shuffle the letters, \nEnter /rank to see your rank, \nEnter /thresholds to see rank thresholds, \nEnter /quit to quit the program, \nor \nEnter /save to save your progress')

def startMenuDisplay():
    print('Please enter "Start" to begin a game, "Help" for a help page, or "Quit" to leave the game.')

def newGameDisplay():
    clearScreen()
    print('Welcome to the start page, enter "Random" to start from a random word, "Load" to start from a save file, or "Choose" to started from your own Chosen word.')
    print('Or enter "Back" to go back to the start page')

def unrecognizedCommand():
    clearScreen()
    print('Unrecognized command, please try again:')

def helpMenuDisplay():
    clearScreen()
    print('---------------------------------------------------------------------------------------------------------------------')
    print('Help Page:\n')
    print('This is a spelling bee game, the objective of the game is to spell out at many words as possible.\n')
    print('A word must be at least 4 and at most 15 letters, use only letters from the 7 on screen, and contain a given special letter.')
    print('You can reuse letters as many times as you need to create your word.')
    print('As you find more words, you will get points which correspond to higher ranks.\n')
    print('Points:')
    print('4 Letter word = 1 point. 5 Letter word = 5 points. 6 Letter word = 6 points, and so on.')
    print('Use all 7 letters given = 7 extra points.')
    print('Rank thresholds are based on the total number of words possible.')
    print('---------------------------------------------------------------------------------------------------------------------\n')
    print('Press enter to continue!')

def returnGuessing():
    print('Press any key to return to guessing.')

def getWords(puzzle):
    print ('Words found:')
    for i in puzzle.getFoundWordList():
        print (i)