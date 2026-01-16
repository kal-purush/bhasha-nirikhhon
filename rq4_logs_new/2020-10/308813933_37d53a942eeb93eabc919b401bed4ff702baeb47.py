import random
import math 

#taking Inputs for lower bound
lower = int(input("Enter lower bound:- "))

#taking inputs for upper bound
upper = int(input("Enter Upper bound:- "))

#generating random number
#between the lower and upper
x = random.randint(lower, upper)
print("\n\tYou've only ", round(math.log(upper - lower + 1, 2))," chances to guess the integer!\n")

#initializing the number of guesses. 
count = 0 

#for calculation of minimum number of 
#guesses depends upon range
while count < math.log(upper - lower + 1, 2): 
    count += 1

    guess = int(input("Guess a number:- "))

    #condition testing 
    if x == guess: 
        print("Congratulations you did it in ", count, " tries")
        #once guessed, the loop will break
        break 
    elif x > guess: 
        print("You guessed to small")
    elif x < guess: 
        print("You guessed to high")

#If guessing is more than required guesses, 
#show this output. 
if count >= math.log(upper - lower + 1, 2): 
    print("\nThe number is %d"%x)
    print("\tBetter Luck Next time!")


