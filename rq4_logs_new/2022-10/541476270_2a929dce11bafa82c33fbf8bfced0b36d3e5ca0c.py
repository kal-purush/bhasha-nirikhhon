#Random number guessing game range 1 - 100

import random

flag = True
while flag:
    num = input('Type a number for the maximum guessing range:')

    if num.isdigit():
        print("Let's play a number guessing game!")
        num = int(num)
        flag = False
    else:
        print("Invalid input! Try again!")

secret = random.randint(1, num)

guess = None
count = 1

while guess != secret:
    guess = input("Please guess the random number between 1 and " + str(num) + ": ")
    if guess.isdigit():
        guess = int(guess)

    if guess == secret:
        print("You guessed right!")
    else:
        print("Please try again!")
        count += 1

print("It took you", count, "guesses!")