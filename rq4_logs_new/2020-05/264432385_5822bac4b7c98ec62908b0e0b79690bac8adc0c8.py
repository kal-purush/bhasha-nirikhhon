import random 


def create_board():
    return([['_', '_', '_'],
                      ['_', '_', '_'],
                      ['_', '_', '_']])

def make_play(board, row, col,token):
    board[row][col] = token
    
#checks if the user chose a valid slot to make the play
def is_valid_play(board, row, col):
    if board[row][col] == '_':
        return True
    else:
        return False
    
def user_turn(board):
    print("Enter your move: ")
    row, col = map(int,input().split())
    if is_valid_play(board, row, col):
        make_play(board, row, col,'X')
    else:
        print('Invalid move, try again!')
        user_turn(board)
        
def pc_turn(board):
    poss = []
    for row, items in enumerate(board):
        for col, item in enumerate(items):
            if item=='_':
                poss.append([row, col])
    row, col = random.choice(poss)
    make_play(board, row, col, 'O')
    
def check_win(board, token):
    if row_check(board, token) or col_check(board, token) or diag_check(board, token):
        return True
    return False

#checks for a complete row 
def row_check(board, token):
    for row in board:
        result = True
        for item in row:
            if item != token:
                result = False
                break
        if result == True:
            return True
    return False

#checks for a complete column
def col_check(board, token):
    for row in range(len(board)):
        result = True
        for col in range(len(board)):
            if board[col][row] !=token:
                result = False
                break
        if result == True:
            return True
    return False

#checks for a complete diagonal
def diag_check(board, token):
    px, py = 0, 0
    result = True
    for _ in range(len(board)):
        if board[px][py] != token:
            result = False
            break
        px+=1
        py+=1
    if result == True:
        return True
    px = len(board)-1
    py = 0
    for _ in range(len(board)):
        if board[px][py] != token:
            result = False
            break
        px-=1
        py+=1
    if result == True:
        return True
    return False

def print_board(board):
    for row in board:
        print('| ',end='')
        for item in row:
            print(item, end = ' | ')
        print('\n')
        
#driver code
board = create_board()
curr_player = random.choice(['X','O'])
win = False
while win == False:
    save_curr_player = curr_player
    print("Current play by ", save_curr_player)
    if curr_player == 'X':
        user_turn(board)
        curr_player = 'O'
    elif curr_player == 'O':
        pc_turn(board)
        curr_player = 'X'
    print("Board :")
    print_board(board)
    win = check_win(board, save_curr_player)
player_dict = {'X':"You", 'O':"PC"}
print(player_dict[save_curr_player]+" won")