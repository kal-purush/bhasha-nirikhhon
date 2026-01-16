# import th erandom package so that we can generate random numbers
from random import randint

# choices is as array => a container that can hold multiple items
# arrays are 0-based => th efirst item is 0, the second is 1, etc 
choices = ["Rock", "Paper", "Scissors"]

# make the computer choose a weapon from the coices array at random
computer_choice = choices[randint(0,2)]

#print the choice to the terminal window
print("Computer chooses: ",computer_choice)