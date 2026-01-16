print("Welcome to the quiz gaming world..!!")

user_input = input("Do you want to proceed to play?  ")
score = 0

if user_input.lower() != "yes":
  quit()

print("Okay let's play :) ")

question_01 = input("What is a correct syntax to output 'Hello World' in Python? ")
if question_01.lower() == "print":
    print("Correct answer!")
    score += 1
  
else:
    print("Wrong answer")

question_02 = input("How do you insert COMMENTS in Python code? ")
if question_02.lower() == "#":
    print("Correct answer!")
    score += 1
  
else:
    print("Wrong answer")   

question_03 = input("Which one is NOT a legal variable name?(my-var/_myvar/Myvar/my_var) ")
if question_03.lower() == "my-var":
    print("Correct answer!")
    score += 1
  
else:
    print("Wrong answer")

question_04 = input("What is the correct file extension for Python files? ")
if question_04.lower() == ".py":
    print("Correct answer!")
    score += 1
  
else:
    print("Wrong answer")

question_05 = input("In Python, 'Hello', is the same as 'Hello'?(True/False) ")
if question_05.lower() == "true":
    print("Correct answer!")
    score += 1
  
else:
    print("Wrong answer")
print("-------------------------------")


print(f"Your correct answer is {score}")
print(f"Your winning percentage is {(score/5)*100} %")