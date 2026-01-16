#!/usr/bin/env python3

# bulls and cows
# Tvým úkolem bude vytvořit program, který by simuloval hru Bulls and Cows. Hra funguje následovně:
#
#     1 Počítač vygeneruje tajné 4místné číslo. Každá číslice tohoto čísla musí být jiná.
#     2 Počítač vždy vyzve uživatele, aby hádal toto číslo.
#     3 Počítač vyhodnotí tip uživatele a vrátí počty shod.
#     4 Pokud uživatel uhádne správné číslo i správnou pozici, jedná se o "bulls".
#       Pokud je pozice jiná, ale číslice je správná, jedná se o "cows".
#
# Příklad hry s číslem 2017
#
# Hi there!
# I've generated a random 4 digit number for you.
# Let's play a bulls and cows game.
# Enter a number
# >>> 1234
# 0 bulls, 2 cows
# >>> 6147
# 1 bull, 1 cow
# >>> 2417
# 3 bulls, 0 cows
# >>> 2017
# Correct, you've guessed the right number in 4 guesses!
# That's {amazing, average, not so good, ...}
#
# Program toho může umět víc. Můžeš přidat například:
#
#     vrácení času, tedy jako dlouho uživateli trvalo číslo uhodnout,
#     uchovávání statistik počtu odhadů jednotlivých her.

from random import seed, randint


def set_rand_num_len():
    while True:
        try:
            rand_num_len = int(input("Type length of your secret number: "))

            if rand_num_len <= 10 and rand_num_len != 0:
                return rand_num_len
            elif rand_num_len == 0:
                print("Hold your horses, partner! How you want guess 0 digit number?")
                continue
            else:
                print("Hold your horses, partner! Your secret number length must have 10 digits or less")
                continue

        except ValueError:
            print("Hold your horses, partner! This is gibberish, not a number!")
            continue


def generate_number(rand_num_len):
    seed()
    rand_number = []

    while len(rand_number) < rand_num_len:
        number = str(randint(0, 9))
        if number not in rand_number:
            rand_number.append(number)

    return rand_number


def check_valid_number(user_in):
    try:
        test_num = int(user_in)
        return True
    except ValueError:
        return False


def user_input(num_len):
    while True:

        user_num_raw = input("Your number: ")
        if check_valid_number(user_num_raw):

            user_num_list = list(user_num_raw)
            print(user_num_list)
            if len(user_num_list) < num_len:
                print(
                    f"Hold your horses, partner! You aim for {num_len} digits number. You number is too short. You need a plan!")
                continue
            elif len(user_num_list) > num_len:
                print(
                    f"Hold your horses, partner! You aim for {num_len} digits number. You number is too long. You need a plan!")
                continue
            else:
                return user_num_list

        else:
            print("Hold your horses, partner! This is gibberish, not a number!")


def check_conditions(user_num, game_num):
    score = {"bulls": 0, "cows": 0}
    for u, g in zip(user_num, game_num):

        if u == g:
            score["bulls"] += 1
        elif u in game_num:
            score["cows"] += 1

    return score


def print_welcome(length):
    print_score("Hello cowboy!")
    print(f"I've generated a random {length} digit number for you.")
    print("Let's play a bulls and cows game.")


def print_score(score):
    # dynamic change of bull | bulls
    # if score 1 => bull, cow; else bulls, cows
    pass


def game_loop():
    number_length = set_rand_num_len()
    print("Delka je", number_length)

    user_attempt = 1

    game_number = generate_number(number_length)
    print_welcome(number_length)

    while True:
        print("Game number", game_number)
        user_number = user_input(number_length)

        print("User number", user_number)

        user_score = check_conditions(user_number, game_number)

        print(f"{user_score['bulls']} bulls, {user_score['cows']} cows")

        if user_score["bulls"] == number_length:
            print(f"You win partner! You need {user_attempt} guesses")
            break
        user_attempt += 1


if __name__ == '__main__':
    game_loop()

# tic tac toe