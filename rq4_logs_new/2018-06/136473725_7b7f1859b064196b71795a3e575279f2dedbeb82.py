import random 
suits = ('Hearts', 'Diamonds', 'Spades', 'Clubs')
ranks = ('Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten', 'Jack', 'Queen', 'King', 'Ace')
values = {'Two':2, 'Three':3, 'Four':4, 'Five':5, 'Six':6, 'Seven':7, 'Eight':8, 'Nine':9, 'Ten':10, 'Jack':10,
		'Queen':10, 'King':10, 'Ace':11}

playing = True

class Card():
	def __init__(self,rank,suit):
		self.suit = suit
		self.rank = rank

	def __str__(self):
		return (f"{self.rank} of {self.suit}")


class Deck():
	def __init__(self):
		self.deck = []
		for suit in suits:
			for rank in ranks:
				card_object = Card(rank,suit)
				self.deck.append(card_object)
				
		

	def __str__(self):
		show_deck = ''
		for item in self.deck:
			show_deck += '\n' + item.__str__()
		
		return (show_deck)

	def shuffle(self):
		random.shuffle(self.deck)
		
	def deal(self):
			single_card=self.deck.pop()
			return single_card


class Hand():
	def __init__(self):
		self.cards=[]
		self.value=0
		self.aces=0

	def add_card(self,card):
		self.cards.append(card)
		self.value+=values[card.rank]
		if card.rank == 'Ace':
			self.aces+=1

	def adjust_for_ace(self):
		if self.value > 21 and self.aces:
			self.value -= 1
			self.aces -+ 1


class Chips():
	def __init__(self,total,bet):
		self.total = total
		self.bet = 0

	def win_bet(self):
		self.total += self.bet

	def lose_bet(self):
		self.total -= self.bet


def take_bet(chips_object):
	while True:
		try:
			bet_money = int(input("Enter your betting amount..?  "))
			chips_object.bet = bet_money
		except:
			print ("Invalid Amount")
		else:
			if bet_money >= chips_object.total:
				print("Betting money cannot exceed ",chips_object.total)
			else:
				break

def hit(deck_object,hand_object):
	hand_object.add_card(deck_object.deal())
	hand_object.adjust_for_ace()

def hit_or_stand(deck_object,hand_object):
	global playing

	while True:
		inp = input("\nWound you want to Hit or Stand? Enter h or s ? ")
		if inp == 'h':
			hit(deck_object,hand_object)
			break

		elif inp == 's':
			print("Player stands, dealer PLAYS. ")
			playing = False
			break

		else:
			print("Sorry, Invalid input Try Again!")


def show_some(hand_player,hand_dealer):
	print("\nDealers Hand:")
	print("<card Hidden> ")
	print(hand_dealer.cards[1])
	print("\nPlayers Hand: ", *hand_player.cards, sep='\n')
	print("\nPlayers Value is: ",hand_player.value)

def show_all(hand_player,hand_dealer):
	print("\nDealers Hand: ", *hand_dealer.cards, sep='\n')
	print("\nDealers Value is: ",hand_dealer.value)
	print("\nPlayers Hand: ", *hand_player.cards, sep='\n')
	print("\nPlayers Value is: ",hand_player.value)

def player_busts(hand_player,hand_dealer,chips_object):
	print("Player loses \n")
	chips_object.lose_bet()

def player_wins(hand_player,hand_dealer,chips_object):
	print("Player Wins!! \n")
	chips_object.win_bet()

def dealer_busts(hand_player,hand_dealer,chips_object):
	print("Dealer loses \n")
	chips_object.win_bet()

def dealer_wins(hand_player,hand_dealer,chips_object):
	print("Dealer Wins!! \n")
	chips_object.lose_bet()

def push():
	print("Its a TIE")

#Running The Play

while True:
	print("Welcome To BLACKJACK! Get as close to 21 as possible without getting over it.\n The Dealer HITS until she gets 17 or more. Aces count as 1 or 11.")

	deck = Deck()		#Deck object created
	deck.shuffle()

	amt = int(input("Enter the initial total money ? "))
	chip = Chips(amt,0)		#Chips object created

	take_bet(chip)

	Player = Hand()
	Dealer = Hand()

	x1=deck.deal()				#first card for player dealt
	Player.add_card(x1)				#first card added for Player in the cards[] list
	x2=deck.deal()				#second card for player dealt
	Player.add_card(x2)				#second card added for Player in the cards[] list
										#ALL HAPPENING ON THE SAME DECK HENCE THE SAME DECK OBJECT 'deck'
	y1=deck.deal()				#first card for dealer dealt
	Dealer.add_card(y1)				#first card added for dealer in the cards[] list
	y2=deck.deal()				#second card for player dealt
	Dealer.add_card(y2)				#second card added for deaker in the cards[] list 	

	show_some(Player,Dealer)

	while playing:						#The loop runs as long as the player wants to hit and the cards from the 
		hit_or_stand(deck,Player)		#deck get added to the cards of the player
		show_some(Player,Dealer)
		if Player.value > 21:
			player_busts(Player,Dealer,chip)
			break

	if Player.value <= 21:
		while Dealer.value < 17:
			hit(deck,Dealer)

		show_all(Player,Dealer)

		if Dealer.value > 21:
			dealer_busts(Player,Dealer,chip)

		elif Dealer.value < Player.value:
			player_wins(Player,Dealer,chip)

		elif Dealer.value > Player.value:
			dealer_wins(Player,Dealer,chip)

		else :
			push()

	print("Players Winning Stand at :", chip.total)

	new_game = input("Would You like to play again y or n  ?  ")

	if new_game == 'y':
		continue

	else: 
		break

