# printing hello
print("Hello")

# 2 to the power of 5
print(2**5)

# the remainder when 6 is divided by 2
print(6 % 2)

# printing the numbers between 1 and 10, i.e. 1, 2, 3, 4, 5, 6, 7, 8, 9
for i in range(1, 10):
  print(i)

# a string that prints your favorite number
"hello"
'What is up?'
print("What is up?")
print("10")
print(10)

# integers versus strings
print("abcd" + "e") # concatenation, putting two strings together, expect "abcde"
print("10" + "1") # combining strings, adding two strings
print(10 + 1) # adding numbers
# print("10" + 1) # is an error! 
# print(10 + "1") # is an error!

# Variables and types
message = "My favorite number is " # a string
print(message)
number = 10 # an integer
print(number)
print(str(number)) # changes 10 to "10"
print(message + str(number))


# task
message = "The answer to everything is"
space = " "
number = 42
print(message + space + str(number))
print(message + " " + str(number))

# bio
print("My name is Marcus.")
print("I live in Colorado.")
print("I am a PhD student.")

# bio rubin?
print("My name is Rubin.")
print("I live in New York City.")

print("THE END")