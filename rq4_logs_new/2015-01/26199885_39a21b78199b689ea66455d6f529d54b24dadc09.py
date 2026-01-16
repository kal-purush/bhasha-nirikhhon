#This will print the menu
#Takes no Parameters
def printMenu():
	print("1: Add")
	print("2: Subtract")
	print("3: Multiply")
	print("4: Divide")
	print("5: Reverse a Phrase")
	print("6: Exit")

#This will ask for the user input and save it as a variable
def userInput():
	temp = input("Your Choice: ")
	return temp

#Add call. Ask for 2 more numbers and display output
def addCall():
	num1 = 0
	num2 = 0
	total = 0
	print("You have chosen Add Num 1 + Num2")
	num1 = float(input("Enter Number 1: "))
	num2 = float(input("Enter Number 2: "))
	total = num1 + num2
	print("Total is {}".format(total))

#Sub will subtrack the two numbers
def subCall():
	num1 = 0
	num2 = 0
	total = 0
	print("You have chosen Subtract. Num 1 - Num2")
	num1 = float(input("Enter Number 1: "))
	num2 = float(input("Enter Number 2: "))
	total = num1 - num2
	print("Total is {}".format(total))

#Multiply will multipy the two numbers
def multiCall():
	num1 = 0
	num2 = 0
	total = 0
	print("You have chosen Multiply. Num 1 * Num2")
	num1 = float(input("Enter Number 1: "))
	num2 = float(input("Enter Number 2: "))
	total = num1 * num2
	print("Total is {}".format(total))

#Divide will divide two numbers and show the remainder
def divideCall():
	num1 = 0
	num2 = 0
	total = 0
	rem = 0;
	print("You have chosen Divide. Num 1 / Num2")
	num1 = float(input("Enter Number 1: "))
	num2 = float(input("Enter Number 2: "))
	total = num1 / num2
	rem = num1 % num2
	print("Total is {} and the remainder is {}".format(total, rem))


#Reverse string will take a string and output the reverse
def reverseString():
	string = ""
	print("You have chosen reverse a phrase! ")
	string = input("Enter your phrase: ")
	reverseString = string[::-1]
	print("Your phrase {} reversed is {}".format(string, reverseString))


print("Hello and Welcome to Python Calculator")

print("Choose an option!")
printMenu()

userChoice = int(userInput())

while(userChoice != 6):
	if(userChoice == 1):
		addCall()
	if(userChoice == 2):
		subCall()
	if(userChoice == 3):
		multiCall()
	if(userChoice == 4):
		divideCall()
	if(userChoice == 5):
		reverseString()
	print("")
	printMenu()
	userChoice = int(userInput())