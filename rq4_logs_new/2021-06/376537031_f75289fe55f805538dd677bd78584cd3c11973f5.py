# function to draw matrix
def draw(M):
    k = 0
    title_ = "    0   " + " 1   " + " 2   "
    print (title_)
    for k in range(3):
        print (k, M[k])
# function to enter the data
def enter (P) :
    incorrect = True
    while incorrect:
        invalid_ = True
        while invalid_:
            try:
                position = list(map(int, raw_input(P + ", please enter your choice: ").split(',')))
                break
            except ValueError:
                print("this is not valid input, try again")
        if position[0] < 3 and int(position[1]) < 3 and position not in entered_P1 and position not in entered_P2:
                incorrect = False
                if P == P1:
                    entered_P1.append(position)
                    return (position)
                if P == P2:
                    entered_P2.append(position)
                    return (position)
        else:
            print ("Wrong input, " + P + ", please try again")

# function to switch players
def switch_player(P, P1, P2):
    if P == P1:
        return P2
    else:
        return P1

def game_over (entered_P):
    i = 0
    sum_row = 0
    sum_col = 0
    sum_d1 = 0
    sum_d2 = 0
    for i in entered_P:
        sum_row = sum_row + i[0]
        sum_col = sum_col + i[1]
    if sum_row == 3 or sum_col == 3:
        return (True)

#**************MAIN MODULE***************************************
print ("________________________________________") # divider
print ("Welcome to Tic-Tac-Toe!") # welcome message
M = [["-" for j in range(3)] for i in range(3)] # initialization of matrix
draw(M) # initial drawing
P1 = raw_input("Enter player one name: ") # name of player one
same = True
while same:
    P2 = raw_input("Enter player two name: ") # name of user two
    if P1 == P2:
        print ("Names must be different")
        P2 = raw_input("Please re-enter player two name: ")  # name of user two
    same = False
print ("***********Alright, The Game begins!**************") # title messages
print ("Players have to enter their choices one after another")
print ("Input must be entered as x,y separated by comma, where x is row and y is column")
game_ = True
P = P1
entered_P1 = []
entered_P2 = []
while game_:
    ent_ = enter(P) # entering user input
    if P == P1: # first user is playing X, second user is playing O
        M[ent_[0]][ent_[1]] = 'X'
    else:
        M[ent_[0]][ent_[1]] = 'O'
    draw(M) # drawing current state
    if game_over(entered_P1):
        game_ = False
        print ("")
        print ("********** Game is over, " + P1 + " won! ************")
        print ("")
        print ("Thank you for playing Tic-Tac-Toe")
        print ("Copyright (C) Dmitri Litvine production. All rights reserved. ")
    if game_over(entered_P2):
        game_ = False
        print ("")
        print ("********** Game is over, " + P2 + " won! ************")
        print ("")
        print ("Thank you for playing Tic-Tac-Toe")
        print ("Copyright (C) Dmitri Litvine production. All rights reserved. ")
    P = switch_player(P, P1, P2) # switching players
    if len(entered_P1)+len(entered_P2) >= 9:
        print ("")
        print ("**********Game is over, no one won************")
        print ("")
        print ("Thank you for playing Tic-Tac-Toe")
        print ("Copyright (C) Dmitri Litvine production. All rights reserved. ")
        game_= False