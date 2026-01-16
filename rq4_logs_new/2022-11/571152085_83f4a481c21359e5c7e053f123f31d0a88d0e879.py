from random import random, randint



EMPTY = " "
playboard = [
    [" ", " ", " "],
    [" ", " ", " "],
    [" ", " ", " "],
]


comp_lable = ""
player_lable = input("Choose lable (X or O): ")
player_lable = player_lable.upper()




computer_wins = 0 



player_wins = 0


draws  = 0 


def print_playboard():
    for i in range(len(playboard)):
        for t in range(len(playboard)):
            print(f"{[playboard[i][t]]}", end="")
        print()










def player_move():
    play_options = [(1,0), (0,0),(0,1),(0,2),(1,1),(1,2),(2,0),(2,1),(2,2)]

    player_move = input(f"Your ({player_lable}'s) turn Enter move (x, y) eg {play_options[randint(0, len(play_options) - 1)]} : ")
    player1_x = int(player_move.split(",")[0])
    player1_y = int(player_move.split(",")[1])
    if not Check_filledBoard():
        while True:
            if check_move(player1_x, player1_y):
                playboard[player1_x][player1_y] = player_lable
                break
            else:
                player_move = input(f"Your ({player_lable}'s) turn Enter move (x, y) eg {play_options[randint(0, len(play_options) - 1)]} : ")
                player1_x = int(player_move.split(",")[0])
                player1_y = int(player_move.split(",")[1])
    else:
        return False
    return int(player_move.split(",")[0]) , int(player_move.split(",")[1])









def computer_move():
    computer_x = int(round((random() * 2 ), 0))
    computer_y = int(round((random () * 2), 0))
    print(f"Computer {comp_lable} plays ({computer_x}, {computer_y})....")
    if not Check_filledBoard():
        while True:
            if check_move(computer_x, computer_y):
                playboard [computer_x] [computer_y] = comp_lable
                break
            else:
                computer_x = int(round((random() * 2 ), 0))
                computer_y = int(round((random () * 2), 0))
    else:
        return False
    return computer_x, computer_y






def check_move(x, y):
    if playboard[x][y] != " ":
        return False
    else:
        return True




def Check_filledBoard():
    for i in playboard:
        for t in i:
            if  t ==" ":
                return False
            else:
                continue
    return True


def check_win(player_move):
    if (playboard[0][0] == player_move and playboard[0][1] ==player_move and playboard[0][2] ==player_move):
        return True
    
    elif (playboard[1][0] ==player_move and playboard[1][1] ==player_move and playboard[1][2] ==player_move):
        return True
    elif (playboard[2][0] ==player_move and playboard[2][1] ==player_move and playboard[2][2] ==player_move):
        return True
    elif (playboard[0][0] ==player_move and playboard[1][0] ==player_move and playboard[2][0] ==player_move):
        return True
    elif (playboard[0][1] ==player_move and playboard[1][1] ==player_move and playboard[2][1] ==player_move):
        return True
    elif (playboard[0][2] ==player_move and playboard[1][2] ==player_move and playboard[2][2] ==player_move):
        return True
    elif (playboard[0][0] ==player_move and playboard[1][1] ==player_move and playboard[2][2] ==player_move):
        return True
    elif (playboard[0][2] ==player_move and playboard[1][1] ==player_move and playboard[2][0] ==player_move):
        return True
    else:
        return False






while True:
    if player_lable not in ["X", "O"]:
        print("Option not available can only choose X or O")
        player_lable = input("Choose lable (X or O): ")
        player_lable = player_lable.upper()
    elif player_lable == "X":
        comp_lable = "O"
        break
    else:
        comp_lable = "X"
        break



def restPlayboard(playboard): 
    playboard = [
        [" ", " ", " "],
        [" ", " ", " "],
        [" ", " ", " "],
    ]
    return playboard





def playGame(rounds):
    def bothPlayerMoves():
        while True:
            player_move()
            computer_move()
            print_playboard()
            global playboard
            if check_win(player_lable) or check_win(comp_lable) or Check_filledBoard():
                if check_win(player_lable):
                    print("You win")
                    playboard = restPlayboard(playboard)
                    global player_wins
                    player_wins += 1
                    break
                elif check_win(comp_lable):
                    print("Comp wins")
                    playboard = restPlayboard(playboard)
                    global computer_wins
                    computer_wins += 1
                    break
                else:
                    print("Game over board filled. It is a draw")
                    playboard = restPlayboard(playboard)
                    global draws
                    draws += 1 
                    break
            else:
                continue
    for i in range(rounds):
        print("Round {}".format(i +1))
        bothPlayerMoves()


playGame(3)
print(f"Your Wins : {player_wins}")
print(f"Computer wins {computer_wins}")
print(f"Draws :  {draws}")