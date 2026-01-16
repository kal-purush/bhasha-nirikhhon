#No betting yet. Basic Doubling Down in. Can only split pairs once.

def hand_summary (hand):
    hand_text = ""
    hand_value = 0
    aces = 0
    for card_in_hand in hand:
        hand_text+=card_in_hand['rank'] + card_in_hand['suit'] + ", "
        if card_in_hand['value'] == 11:
            aces = aces + 1
        hand_value = hand_value + card_in_hand['value']
    hand_text = hand_text[:-2]
    while aces > 0:
        if hand_value > 21:
            hand_value = hand_value - 10
        aces = aces - 1
    return {"text":hand_text,"value":hand_value}        

def build_deck():
    deck = []
    for index in range (52):
        card_suit = suits[index // 13]
        card_rank = ranks[index % 13 + 1]
        if (index % 13 + 1) == 1:
            card_value = 11
        elif (index % 13 + 1) > 10:
            card_value = 10
        else:
            card_value = (index % 13 + 1)
        card = {"rank":card_rank,"suit":card_suit,"value":card_value}
        deck.append(card)    
    return(deck)
        
suits = {
    0 : " of Diamonds",
    1 : " of Clubs",
    2 : " of Hearts",
    3 : " of Spades",
}
ranks = {
    1 : "Ace",
    2 : "Two",
    3 : "Three",
    4 : "Four",
    5 : "Five",
    6 : "Six",
    7 : "Seven",
    8 : "Eight",
    9 : "Nine",
    10 : "Ten",
    11 : "Jack",
    12 : "Queen",
    13 : "King",
}
import random
player_hand = []
second_hand = []
player_blackjack = False
double_down = ""
#split_pair = ""
dealer_hand = []
dealer_blackjack = False
done = False
#p2done = False
deck=build_deck()
random.shuffle(deck)
player_hand.append(deck.pop())
player_hand.append(deck.pop())
dealer_hand.append(deck.pop())
dealer_hand.append(deck.pop())

#Fixing hand for debugging purposes
#player_hand = [{"rank":"Ten","suit":" of Diamonds","value":10},
#               {"rank":"Ten","suit":" of Clubs","value":10}]
player_hand_summary=hand_summary(player_hand)
second_hand_summary={"text":"","value":22} # for end of game checking logic. Will be overridden in split pairs
dealer_hand_summary=hand_summary(dealer_hand)
#If Dealer has a blackjack, they will win regardless
if dealer_hand_summary["value"] == 21:
    dealer_blackjack = True

# Player blackjack wins other than against Dealer blackjack
if player_hand_summary["value"] == 21:
    player_blackjack = True
    done = True
    print(f"Your Hand: \n{player_hand_summary['text']}\nYour hand's value is:{player_hand_summary['value']}.")
    print("Blackjack!")

# Player blackjack wins other than against Dealer blackjack
if player_hand_summary["value"] == 21:
    player_blackjack = True
    done = True
    print(f"Your Hand: \n{player_hand_summary['text']}\nYour hand's value is:{player_hand_summary['value']}.")
    print("Blackjack!")

#if player_hand[0]["rank"]==player_hand[1]["rank"]:
#    print(f"Your Hand: \n{player_hand_summary['text']}")
#    print(f"The dealer is currently showing: {dealer_hand[0]['rank']}{dealer_hand[0]['suit']}.")
#    while split_pair not in ("y","n"):
#        split_pair = input("Do you want to split your hand? [y/n]: ").lower()
#    if split_pair == "y":
#        second_hand.append(player_hand.pop())
#        player_hand.append(deck.pop())
#        player_hand_summary=hand_summary(player_hand)
#        print(f"Your first Hand: \n{player_hand_summary['text']}\nYour first hand's value is: {player_hand_summary['value']}.")
#        if player_hand_summary['value'] == 21:
#            player_blackjack = True
#            done = True
#            print("Blackjack!")
#        second_hand.append(deck.pop())
#        second_hand_summary=hand_summary(second_hand)
#        print(f"Your second Hand: \n{second_hand_summary['text']}\nYour second hand's value is: {second_hand_summary['value']}.")
#        if second_hand_summary['value'] == 21:
#            player_blackjack = True
#            p2done = True
#            print("Blackjack!")

    
elif player_hand_summary["value"] in (9,10,11):
    print(f"Your Hand: \n{player_hand_summary['text']}")
    print(f"The dealer is currently showing: {dealer_hand[0]['rank']}{dealer_hand[0]['suit']}.")
    
    while double_down not in ("y","n"):
        double_down=input(f"Would you like to Double Down? [y/n]: ").lower()
    if double_down == "y":
        player_hand.append(deck.pop())
        player_hand_summary=hand_summary(player_hand)
        #bet double
        print(f"Your Hand: \n{player_hand_summary['text']}\nYour hand's value is:{player_hand_summary['value']}.")
        done = True
while not done:
    print(f"Your Hand: \n{player_hand_summary['text']}\nYour hand's value is: {player_hand_summary['value']}.")
    print(f"The dealer is currently showing: {dealer_hand[0]['rank']}{dealer_hand[0]['suit']}.")
    action =""
    while action not in ('h','s'):
        print("Would you like to [H]it or [S]tand?")
        action=input().lower()
    if action == "s":
        done = True
    else:
        player_hand.append(deck.pop())
        print(f"You got the {player_hand[-1]['rank']}{player_hand[-1]['suit']}.")
        player_hand_summary=hand_summary(player_hand)
        if player_hand_summary["value"] > 21:
              print(f"You bust at {player_hand_summary['value']}.")
              done = True
#if split_pair == "y":
#    while not p2done:
#        second_hand_summary=hand_summary(second_hand)
#        print(f"Your Hand: \n{second_hand_summary['text']}\nYour hand's value is: {second_hand_summary['value']}.")
#        print(f"The dealer is currently showing: {dealer_hand[0]['rank']}{dealer_hand[0]['suit']}.")
#        action =""
#        while action not in ('h','s'):
#            print("Would you like to [H]it or [S]tand?")
#            action=input().lower()
#        if action == "s":
#            p2done = True
#        else:
#            second_hand.append(deck.pop())
#            print(f"You got the {second_hand[-1]['rank']}{second_hand[-1]['suit']}.")
#            second_hand_summary=hand_summary(second_hand)
#            if player_hand_summary["value"] > 21:
#                print(f"You bust at {second_hand_summary['value']}.")
#                p2done = True            
            
            
            
if player_blackjack and dealer_blackjack:
    print(f"The Dealer has: \n{dealer_hand_summary['text']}\nAlso a blackjack!\nIt's a tie.")
elif player_blackjack:
    print("The Dealer does not have blackhack\nYou win!")
elif dealer_blackjack:
    print(f"The Dealer has: \n{dealer_hand_summary['text']}\nBlackjack!\nYou lose.")
elif player_hand_summary["value"] <= 21:
    print(f"The Dealer has: \n{dealer_hand_summary['text']}\nTheir hand's value is: {dealer_hand_summary['value']}.")
    while dealer_hand_summary["value"] < 17:
        dealer_hand.append(deck.pop())
        print(f"The dealer hits\nThe dealer got the {dealer_hand[-1]['rank']}{dealer_hand[-1]['suit']}")
        dealer_hand_summary=hand_summary(dealer_hand)
    if dealer_hand_summary["value"] > 21:
        print(f"The dealer busts with {dealer_hand_summary['value']}.\nYou win!")
    else:
        print(f"The dealer stands with {dealer_hand_summary['value']}.")
        if player_hand_summary["value"] > dealer_hand_summary["value"]:
            print(f"Your hand of {player_hand_summary['value']} beats the Dealer.")
        #if second_hand_summary['value'] > 22 and second_hand_summary['value'] > dealer_hand_summary['value']:
        #    print(f"Your second hand of {second_hand_summary['value']} beats the Dealer.")
        elif dealer_hand_summary["value"] > player_hand_summary["value"]:
            print(f"You lose!")
        else:
            print("It's a tie.")
else:
    print("You lose!")