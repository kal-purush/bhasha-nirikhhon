from random import randint
from time import sleep

arr = [0, 1, 2, 4, 8]
inputs = []
num_to_solve = randint(2,15)

def solve():
    user_inputFinal = inputs[0] + inputs[1] + inputs[2] + inputs[3]
    if user_inputFinal == num_to_solve:
        print("Well done")
    else: 
        print("Wrong Number")
        sleep(3)
        exit()


#check for user input 4
def user_inputFour():
    user_input4 = int(input("Fourth Number: "))
    if user_input4 in arr:
        inputs.append(user_input4)
        solve()
    else:
        print(f"{user_input4} is not in {arr} therefore it is invalid")
        exit()

#check for user input 3
def user_inputThree():
    user_input3 = int(input("Third Number: "))
    if user_input3 in arr:
        inputs.append(user_input3)
        user_inputFour()
    else:
        print(f"{user_input3} is not in {arr} therefore it is invalid")
        exit()

#check for user input 2
def user_inputTwo():
    user_input2 = int(input("Second Number: "))
    if user_input2 in arr:
        inputs.append(user_input2)
        user_inputThree()
    else:
        print(f"{user_input2} is not in {arr} therefore it is invalid")
        exit()




print(num_to_solve)
print(f"Type the numbers either 1,2,4,8 to get the {num_to_solve} because all the numbers you pick will be added together otherwise type 'exit' to exit")
print("------------------------------------------------------")
#check fore user input 1
user_input1 = int(input("First Number: "))
if user_input1 in arr:
    inputs.append(user_input1)
    user_inputTwo()
else:
    print(f"{user_input1} is not in {arr} therefore it is invalid")
    exit()



