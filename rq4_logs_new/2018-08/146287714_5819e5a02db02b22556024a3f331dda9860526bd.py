#This is a comment.
"""This is a
multiline docstring."""
"""

print("Hello, World!")
if 5 > 2:
    print("Five is greater than two!")

x = 5
y = "John"
print(x)
print(y)

x = 4 # x is of type int
x = "Sally" # x is now of type str
print(x)

x = ("awesome")
print("Python is " + x)
print ("Python is %s." % x)
print ("Python is", x)

x = "Python is "
y = "awesome"
z =  x + y
print(z)

x = 1
y = 35656222554887711
z = -3255522

print(type(x))
print(type(y))
print(type(z))

x = 1.10
y = 1.0
z = -35.59

print(type(x))
print(type(y))
print(type(z))

x = 3+5j
y = 5j
z = -5j

print(type(x))
print(type(y))
print(type(z))


x = str("s1")
y = int(2)
z = float(3.0)
print(x)
print(y)
print(z)

print("Enter your name:")
x = input()
print("Hello, " + x)
a = "Hello, World!"
print(a[1])
print(a[2:5])
print(len(a))
print(a.lower())
print(a.replace("H", "J"))
print(a.split(","))
#A list is a collection which is ordered and changeable. In Python lists are written with square brackets.
thislist = ["apple", "banana", "cherry"]
thislist[1] = "blackcurrant"
print(thislist)
#A tuple is a collection which is ordered and unchangeable. In Python tuples are written with round brackets.
thistuple = ("apple", "banana", "cherry")
# thistuple[1] = "blackcurrant" Error
print(thistuple)
#A set is a collection which is unordered and unindexed. In Python sets are written with curly brackets.
thisset = {"apple", "banana", "cherry"}
print(thisset)
#A dictionary is a collection which is unordered, changeable and indexed. In Python dictionaries are written with curly brackets, and they have keys and values.
thisdict = {
  "apple": "green",
  "banana": "yellow",
  "cherry": "red"
}
print(thisdict)
thisdict["apple"] = "red"
print(thisdict)

thisdict = dict(apple="green", banana="yellow", cherry="red")
# note that keywords are not string literals
# note the use of equals rather than colon for the assignment
print(thisdict)

a = 200
b = 33
c = 10
if b > a:
  print("b is greater than a")
elif a == b:
  print("a and b are equal")
else:
  print("a is greater than b")
if a > b and c > a:
    print("Both conditions are True")
if a > b or a > c:
  print("At least one of the conditions are True")

i = 1
while i < 6:
  print(i)
  if i == 3:
    break
  i += 1

fruits = ["apple", "banana", "cherry"]
for x in fruits:
  if x == "banana":
    continue
  print(x)

for x in range(6):
  print(x)
#Increment the sequence with 3 (default is 2):
for x in range(1, 30, 3):
  print(x)

def tri_recursion(k):
  if(k>0):
    result = k+tri_recursion(k-1)
    print(result)
  else:
    result = 0
  return result
print("\n\nRecursion Example Results")
tri_recursion(6)

def my_function():
  print("Hello from a function")
my_function()
def my_function(x):
  return 5 * x
print(my_function(3))
x = lambda a : a + 10
print(x(5))
x = lambda a, b, c : a + b + c
print(x(5, 6, 2))
"""
#Arrays https://www.w3schools.com/python/python_arrays.asp
cars = ["Ford", "Volvo", "BMW"]
x = len(cars)
print(x)
"""
https://www.w3schools.com/python/python_classes.asp
https://www.w3schools.com/python/python_iterators.asp
https://www.w3schools.com/python/python_modules.asp
https://www.w3schools.com/python/python_datetime.asp
https://www.w3schools.com/python/python_json.asp
https://www.w3schools.com/python/python_pip.asp
https://www.w3schools.com/python/python_try_except.asp
"""