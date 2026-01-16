###############################################################
# CS 21A Programming in Python Lab #2
#
# Variables, Integers, Float, Print statements, Formated Output.
# Calculate different expressions using the sum student id and
# the number of letters in your last name.
# 
###############################################################
# Read the input from the user
myName = (input("Enter your family name: "))
studentId = (input("Enter your student ID: "))

# Create myId and nLet variables
myId = 0
for i in range(0, len(studentId)) :
    myId += int(studentId[i])
nLet = len(myName)

print("myId is: ", myId)
print ("nLet is: ", nLet)

#expression 1
result = myId / 2
print("expression 1: %5.2f" % result)
#expression 2
result = myId % 2
print("expression 2: ", result)

#calculate expression 3 summand
result = 0
for i in range (2, nLet + 1) :
    result += i

print("expression 3: ", result)
#expression 4
result = myId + nLet;
print("expression 4: ", result)
#expression 5
result = abs(nLet - myId)
print("expression 5: ", result)
#expression 6
result = (myId) / (nLet + 1100)
print("expression 6: %5.2f" % result)
#expression 7
result = (nLet % nLet) and (myId * myId)
print("expression 7: ", result)
#expression 8
result = 1 or (myId/0)
print("expression 8: ", result)
#expression 9
result = round(3.14, 1)
print("expression 9: %5.2f" % result)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Enter your family name: Tyler
# Enter your student ID: 12345
# myId is:  15
# nLet is:  5
# expression 1:  7.50
# expression 2:  1
# expression 3:  14
# expression 4:  20
# expression 5:  10
# expression 6:  0.01
# expression 7:  0
# expression 8:  1
# expression 9:  3.10
##################################################################