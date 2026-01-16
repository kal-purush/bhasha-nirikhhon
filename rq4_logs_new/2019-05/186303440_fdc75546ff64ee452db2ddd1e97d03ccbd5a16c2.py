name = "Marcus"   # variable! 
print("Marcus")
print(name)  # cubbyhole

# you can change the content of variables
# and they update as you see them
miles = 42.5
print(miles)
miles = 56.1
print(miles)


# not allowed variable name: 9miles 
# cannot have: <> ? / \ ? ! & #
# legal variable names:
# variable
# x 
# y 
# z
# this_is_my_name
# _variable

age = 17
my_future_age = 18
print(str(age) + " " + str(my_future_age))
print(age, my_future_age)

aa = bb = cc = 1
print(aa, bb, cc)

my_name, your_name = "Marcus", "Rubin"
my_name = "Marcus"
your_name = "Rubin"
print(my_name, your_name)


# data types
# strings
my_string = "string"

# numbers
my_integer = 1
my_float = 1.0
print(my_integer + my_float)

# lists
my_list = [17, 18, 17, 16, 20]
my_list2 = [[42, 0], [88, -15], [54, 2]]
print(my_list)

# sets
my_set = set(my_list)
print(my_set)

# dictionary
my_dictionary = {"Rubin": 17, "Pablo": 18, "Veronica":16, "Philmore":20}
print(my_dictionary)
print("How old is Rubin?", my_dictionary["Rubin"])

# tuple
my_tuple = (17, 18, 16)

# boolean 
my_bool = True

# Calculate the volume of a box, price, weight
width = 7 # inches
height = 11 # inches
depth = 5 # inches
volume = width * height * depth
print("volume", volume, "cubic inches")
price_per_volume = 0.055 # dollars per cubic inch
price = price_per_volume * volume
print("$", price)
print("${:.2f}".format(price))
print("{:.3f}, {:.5f}, my favorite though is {:.0f}".format(13, 14, 42))


# (a) a-variable-name
# (b) a variable
# (c) A_VARIABLE
a = 27
variable = 45
name = 54
print(a-variable-name)
#a-variable-name = True
# a variable = False
A_VARIABLE = True

address_book = {"Bert": "36 One Way", "Bill":"23 54th Street"}
print(address_book["Bill"])
address_book["Cecilia"] = "123 Red Circle"
print("Cecilia lives at", address_book["Cecilia"])

# Problem 1
# Write a program that will calculate how much Lila will spend on buying new
# jeans. Lila plans to buy 5 pairs of jeans at $10 each. The tax rate is 0.10. You
# must use four variables: name, numberOfJeans, priceOfJeans, and taxRate.
# Use variables like num_jeans, tax_rate, and jean_price
# Output: Lila will spend $51.10 on 5 pairs of jeans.

num_jeans = 5
price_jeans = 10
tax_rate = 0.10
totalprice_jeans = num_jeans * price_jeans * (1 + tax_rate)
print("It will cost Lila", totalprice_jeans, "to buy 5 pairs of jeans.")
print("It will cost Lila ${:.2f} to buy 5 pairs of jeans.".format(totalprice_jeans))

# Problem 2
# Convert fahrenheit to celsius
# formala: (fahrenheit - 32) - 5/9
# output: 45 degrees Fahreneheit is ??? Celsius
fahrenheit = 90
celsius = (fahrenheit - 32) - 5/9 # you figure this out
print("45 degrees Fahrenheit is", celsius, "degrees celsius.")
print("{:.2f} degrees Fahrenheit is {:.2f} degrees celsius.".format(fahrenheit, celsius))