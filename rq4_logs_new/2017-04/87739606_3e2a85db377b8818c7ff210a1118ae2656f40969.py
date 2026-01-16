import random

#Using the random module and the range method, 
#generate a list of 20 random numbers between 0 and 49.
random_20 = random.sample(range(0, 49), 20)
print(random_20)

#use a list comprehension to build a new list that contains each number squared.
squared_random_numbers = [ x**2 for x in random_20 ]
print(squared_random_numbers)