#!/usr/bin/env python3
# Created by: Ferdaws
# Created on: Apr 2022.
# uses a while true loop to
# that doesn't let users exit until they get
# they guess the right number

import random


def main():
    correct_num = random.randint(0,9)
    while True:
        user_guess = input("Guess a number between 0 and 9 : ")
        try:
            user_int = int(user_guess)
            if user_int == correct_num:
                print("Your guessed right")
                break
            else:
                print("You guessed wrong.")
        except Exception:
            print("invalid input")
if __name__ == "__main__":
    main()