"""

Zachary Pajela Application Project - Data Analytics

Assignment:

The Project 

- Write a python program that iterates the integers from 1 to 50. 
- For multiples of three print "Data" instead of the number
- For multiples of seven print "Apprenticeship"
- For numbers which are multiples of both three and seven print "DataApprenticeship"


"""

# Prompt says 1 - 50
y= []

start, end = 1, 50

if start < end:
    y.extend(range(start, end))
    y.append(end)

# Modulus checks remainder == 0
for x in y:
    if x % 7 == 0 and x % 3 == 0:
        print("- DataApprenticeship")
    elif x % 3 == 0:
        print("- Data")
    elif x % 7 == 0:
        print("- Apprenticeship")
    else:  
        print(x)