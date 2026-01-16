import random

from character import *
from battle import *
# import battle.py
from rooms import *
# import enemies.py
# import weapons.py
# import armor.py


# Asks for player's name
print "\nWhat is your name?"
name = raw_input("> ")
print "\nExcellent %s, thanks for playing! \n" % name

player = "None"

# Asks player to choose character type
while player == "None":

	print "Now choose what type of character that you would like to play: \n"
	print "1. Archer \n"
	print "2. Swordsman \n"
	print "3. Thief \n"
	
	player = raw_input("> ")

	if player in ("1", "archer", "Archer"):
		player = Archer(name)
	elif player in ("2", "Swordsman", "swordsman"):
		player = Swordsman(name)
	elif player in ("3", "Thief", "thief"):
		player = Thief(name)
	else:
		print "\nTry something different \n"
		player = "None"

		

battle = Swing(player.type, player.hitpoints, player.armor, player.weapon)
print "\n"

outcome = battle.action()
print outcome