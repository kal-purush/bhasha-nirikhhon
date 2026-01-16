#BlackJackt Project
import random
import os

logo = """
.------.            _     _            _    _            _    
|A_  _ |.          | |   | |          | |  (_)          | |   
|( \/ ).-----.     | |__ | | __ _  ___| | ___  __ _  ___| | __
| \  /|K /\  |     | '_ \| |/ _` |/ __| |/ / |/ _` |/ __| |/ /
|  \/ | /  \ |     | |_) | | (_| | (__|   <| | (_| | (__|   < 
`-----| \  / |     |_.__/|_|\__,_|\___|_|\_\ |\__,_|\___|_|\_\\
      |  \/ K|                            _/ |                
      `------'                           |__/           
"""

print(logo)

def deal_card():
    cards = [11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10]
    card = random.choice(cards)
    return card

def calculate_scores(cards):
    if sum(cards) == 21 and len(cards) == 2:
        return 0
    if 11 in cards and sum(cards) > 21:
        cards.remove(11)
        cards.append(1)
    return sum(cards)

def compare(user_score, computer_score):
    if computer_score == user_score:
        return"It's a Draw 🙃"
    elif computer_score == 0:
        return "Lose! Oponent has a BlackJack 😱"
    elif user_score == 0:
        return "Won! With a BlackJack 😘 "
    elif user_score > 21:
        return "You Lose! 😉"
    elif  computer_score > 21:
        return "You Won! ☺"
    elif user_score > computer_score:
        return "You Won!"
    elif computer_score > user_score:
        return "You Lose!"

def main_loop():
    user_cards = []
    computer_cards = []
    game_end = False

    for _ in range(2):
        user_cards.append(deal_card())
        computer_cards.append(deal_card())

    while not game_end:
        user_score = calculate_scores(user_cards)
        computer_score = calculate_scores(computer_cards)
        print(f"Your cards: {user_cards}, current score: {user_score}")
        print(f"Computers first card: {computer_cards[0]}.")

        if user_score == 0 or computer_score == 0 or user_score > 21:
            game_end = True
        else:
            play_again = input("Would you like to draw another card? Press 'yes' to draw or 'no' to stand.")

            if play_again == "yes":
                user_cards.append(deal_card())
            else:
                game_end = True

    while computer_score != 0 and computer_score < 17:
        computer_cards.append(deal_card())
        computer_score = calculate_scores(computer_cards)

    print(f"Your final hand: {user_cards}, Your final score{user_score}.")
    print(f"Computers final hand: {computer_cards}, Computers final score {computer_score}." )
    print(compare(user_score, computer_score))

while input("Would you like to play a game of BlackJack? Type 'yes' to play or type 'no'.""\n") == 'yes':
  os.system("cls")
  main_loop()
else:
  print('Goodbye!')
  quit()