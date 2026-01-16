
import random

random_number = random.randrange(1, 9)
print("I have a random number.")

guess = int(input("Please guess a number: "))

while random_number != guess:
    if guess < random_number: #if guess is less than
        print("Guess higher.")
        guess = int(input("Please guess a number: "))
    elif guess > random_number: #if guess is greater than
        print("Guess lower")
        guess = int(input("Please guess a number: "))
else: #if guess is equal to
    print("You got it")

