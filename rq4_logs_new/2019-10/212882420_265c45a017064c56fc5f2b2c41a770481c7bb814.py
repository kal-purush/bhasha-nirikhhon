import re

inputStr = "String temp = \"C++ and Python\""

# Check if "int" is present at the beginning of a word:

datatype = re.findall("^String", inputStr)

array = [datatype[0]]

print("Datatype =", array[0])

##### When declaration is like "String temp="Hello!" "
tempStr = inputStr.replace('String', '')

tempStr = tempStr.rsplit("=", 1)
tempStr = tempStr[0].strip()

array.append(tempStr)
print("Variable =", array[1])

print(array)