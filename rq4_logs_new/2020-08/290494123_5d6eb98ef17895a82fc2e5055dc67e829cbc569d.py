import random,math
randomNumber=random.randint(1,9)
print("What is the number you are guessing ")
guessing=input()
if(guessing==randomNumber):
    print("YOU DID IT THE NUMBER WAS:")
    print(guessing)
else:
    print("Sorry That isnt the number")