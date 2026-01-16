#DandD Dice Roller
#
#Rather than brand yourself a nerd use this python based
#D&D die roller. Now you can jump in a game any wher without
#the sitgma of a pound 'o dice.

print ("So you forgot your favorite bag of dice.")
print ("Just enter the number of sides on the required die")
print ("and specify the number of rolls. Adventure awaits!\n")

print ("Incase you forgot:")
print ("D20 is for skill and attack rolls")
print ("D12, D10, D8, D6 are used for damage rolls")
print ("D4 is used for...Well no one really uses D4")
print ("\n")

import random

dice = int(input('What sided dice are you rolling?:'))
rolls = int(input('How many dice are you rolling?:'))
print ("\n")
print ("Rolling a D%s" % (dice), rolls, "times\n")

while rolls !=0:
    die1 = random.randint(1, dice)
    print ("You rolled a D%s" % (dice), "and got a", die1)
    rolls -= 1

print ("\n")
input("Press the enter key to exit.")
