str=input("Enter a String: ")

str = str.lower()

userInput = str.split()

newList = list()

for i in userInput:
    if i not in newList:
        newList.append(i)

print(newList)