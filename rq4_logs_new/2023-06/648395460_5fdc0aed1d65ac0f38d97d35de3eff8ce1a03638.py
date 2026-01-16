import random

MIN_NUMBER = 1
MAX_NUMBER = 10
NB_QUESTIONS = 4

#ask user to compute addition of 2 numbers
#display right answer/wrong answer

#ask_question()
# def ask_question():
#     a = random.randint(MIN_NUMBER, MAX_NUMBER)
#     b = random.randint(MIN_NUMBER, MAX_NUMBER)

#     answer_str = input(f"Compute: {a} + {b} = ")
#     answer_int = int(answer_str) #convert string to int
   
#     if answer_int == a+b:
#       # print("Right Answer")
#        return True
#     #print("Wrong Answer")
#     return False
#    #  else:
#    #     print("Wrong Answer")
#    #     return

#ask question with random calculations
def ask_question():
    a = random.randint(MIN_NUMBER, MAX_NUMBER)
    b = random.randint(MIN_NUMBER, MAX_NUMBER)
    o = random.randint(0, 1)
    #0 = +
    #1 = *
    #create operator str
    operator_str = "+"
    
    #create operator conditions
    if o == 1:
       operator_str = "*"
   

#add operator input
    answer_str = input(f"Compute: {a} {operator_str} {b} = ")
    answer_int = int(answer_str) #convert string to int
   #create intermitent varible 
    compute = a+b
    if o == 1:
       compute = a*b
    
   
    if answer_int == compute:
      # print("Right Answer")
       return True
    #print("Wrong Answer")
    return False


#calculate points
nb_points = 0
for i in range (0, NB_QUESTIONS):
    print(f"Question {i + 1} out of {NB_QUESTIONS}")
    print()
    if ask_question():
       print("Right Answer")
       nb_points += 1
    else:
       print("Wrong Answer")   
    print()

print(f"Your points: {nb_points} out of {NB_QUESTIONS}")

#display score + message
if nb_points == NB_QUESTIONS:
   print("Excellent!")
elif nb_points > NB_QUESTIONS/2:
   print("You can do better")
else:
   print("You have failed")
   