# ## 
# This is going to be a script to play single player poker
# Player will start with a bank of some number of dollars
# Each game will start with the player being told the amount in
# their bank and asked what they want to bet
# if the player loses a hand they lose that bet
# if the player wins they will win some multiplier of the bet
# ##

import random

def main():
	global deck
	global playerHand
	initialDeal()
	discard()
	finalDeal()
	checkWin()
	deck = [ '2H', '3H' ,'4H' , '5H' , '6H' , '7H' ,'8H' ,'9H' ,'10H' , 'JH', 'QH', 'KH' , 'AH' , '2D', '3D' ,'4D' , '5D' , '6D' , '7D' ,'8D' ,'9D' ,'10D' , 'JD', 'QD', 'KD' , 'AD' ,  '2S', '3S' ,'4S' , '5S' , '6S' , '7S' ,'8S' ,'9S' ,'10S' , 'JS', 'QS', 'KS' , 'AS' ,  '2C', '3C' ,'4C' , '5C' , '6C' , '7C' ,'8C' ,'9C' ,'10C' , 'JC', 'QC', 'KC' , 'AC' ]
	playerHand = []
	print('Starting a new game')
	main()
	

#ToDo Define the deck
deck = [ '2H', '3H' ,'4H' , '5H' , '6H' , '7H' ,'8H' ,'9H' ,'10H' , 'JH', 'QH', 'KH' , 'AH' , '2D', '3D' ,'4D' , '5D' , '6D' , '7D' ,'8D' ,'9D' ,'10D' , 'JD', 'QD', 'KD' , 'AD' ,  '2S', '3S' ,'4S' , '5S' , '6S' , '7S' ,'8S' ,'9S' ,'10S' , 'JS', 'QS', 'KS' , 'AS' ,  '2C', '3C' ,'4C' , '5C' , '6C' , '7C' ,'8C' ,'9C' ,'10C' , 'JC', 'QC', 'KC' , 'AC' ]
playerHand = []


#ToDo Define the cards that the player is dealt and display them to the player
def initialDeal():
	for i in range(5):
		end = len(deck) - 1
		playerHand.append(deck.pop(random.randint(0,end)))
	return playerHand

#ToDo Allow the player to discard 0-5 cards

def discard():
	print('Your cards are ' + str(playerHand) + '\n')
	willDiscard = input('Would you like to discard any cards type Y/N : ')
	if willDiscard == 'Y':
		toDiscard = input('Which cards do you want to discard?: ')
		playerHand.remove(toDiscard)
		discard()
	elif willDiscard == 'N':
		print("Alright, let's draw " + str(abs(len(playerHand) - 5)) + " more cards\n\n" )
	else:
		print("Sorry I didn't understand that please type Y or N")
		discard()
		

#ToDo fill players hand back up with 5 cards
def finalDeal():
	toDraw = abs(len(playerHand) - 5)
	end = len(deck) - 1
	for i in range(toDraw):
		playerHand.append(deck.pop(random.randint(0,end)))
	print('Your final hand is ' + str(playerHand))

# ToDo Check final hand for various win conditions
#Win conditions
# Two pair Jacks or better
# Three of a kind
# Four of a kind
# Full house
# Straight
# Flush
# Straight Flush
# Royal Flush
def checkWin():
	if (
('JH' and 'JD') or ('JH' and 'JC') or ('JH' and 'JS') 
or ('JD' and 'JC') or ('JD' and 'JS') or ('JC' and 'JS')
or ('QH' and 'QD') or ('QH' and 'QC') or ('QH' and 'QS') 
or ('QD' and 'QC') or ('QD' and 'QS') or ('QC' and 'QS')
or ('KH' and 'KD') or ('KH' and 'KC') or ('KH' and 'KS') 
or ('KD' and 'KC') or ('KD' and 'KS') or ('KC' and 'KS')
or ('AH' and 'AD') or ('AH' and 'AC') or ('AH' and 'AS') 
or ('AD' and 'AC') or ('AD' and 'AS') or ('AC' and 'AS')
) in playerHand:
		print('Winner, winner, chicken dinner!!\n\n\n\n\n')
	else:
		print('A loser is you\n\n\n\n\n')
#ToDo Score players hand and multiply bet by payout structure

main()