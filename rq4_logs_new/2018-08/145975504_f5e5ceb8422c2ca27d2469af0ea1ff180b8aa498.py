# even more variables and printing
# in this exercise we will embed variables inside a string using the special {} sequence, placing the variable we want inside those braces
# we must also begin the. string with the letter f for "format" as in f"Hello {somevar}"
# the little f before the double quote character lets Python3 (doesnt work in Python2, I tried) know that our string must be formatted when executed

my_name = "A.T. Kell"
my_age = 32 # not a lie
my_height = 5 * 12 + 7 # 5' 7" converted to inches
my_weight = 204 # lbs
my_eyes = "Blue"
my_teeth = "White"
my_hair = "Blonde"

print(f"Let's talk about {my_name}.")
print(f"He's {my_height} inches tall.")
print(f"He's {my_weight} pounds heavy.")
print(f"Hey, hey, hey, that's not too heavy!")
print(f"He's got {my_eyes} eyes and {my_hair} hair.")
print(f"His teeth are usually {my_teeth} depending on the coffee.")

# this line is tricky, try to get it exactly right
total = my_age + my_height + my_weight
print(f"If I add {my_age}, {my_height}, and {my_weight} I get {total}")

# I ran into the following error simply because I had originally written the assignemnt for my_age as a string, my_age = "32" when it should have been as an integer, my_age = 32
# Traceback (most recent call last):
#   File "ex5.py", line 22, in <module>
#     total = my_age + my_height + my_weight
# TypeError: can only concatenate str (not "int") to str
