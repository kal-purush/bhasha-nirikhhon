import datetime
# This is a sample Python script made by German Bobadilla

# Part A
# In PyCharm, write a program that prompts the user for their name and age.
# Your program should then tell the user the year they were born.
# Here is a sample execution of the program with the user input in bold:

# Variables
name = input('What is your name')
date = datetime.date.today()
year = date.year
age = int(input("How old are you"))
year_born = year - age

print(f"Hello\033[1m {name}\033[0m! You were born in\033[1m {year_born - 1}\033[0m.")

# Part B
# I noticed that I could import the datetime library and it did it with ease.
# When I called the datetime function, it was coloured.
# It's better to use an IDE like this with colors.