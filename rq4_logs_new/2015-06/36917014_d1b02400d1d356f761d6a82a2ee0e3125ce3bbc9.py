#  Kent Han 23271274.  ICS 32 Lab sec 8.  Project 4.
import othello_logic

def num(prompt) ->int:
    while True:
        try:
            num = int(input(prompt))
            if num % 2 == 0:
                if 16 >= num >= 4:
                    break
            else:
                print('Try again')
        except:
            print('Not quite')
    return num
def plch(prompt: str) ->str:
    while True:
        pc = input(prompt).upper()
        if pc == 'B':
            break
        elif pc == 'W':
            break
        else:
            print('Oops')
    return pc



def init_dict() ->dict:
    tlprompt = 'Who goes in the top left? Enter B for Black or W White: '
    m1prompt = 'Who goes first? Enter B for Black or W White: '
    rowprompt = 'How many rows?(4-16): '
    colprompt = 'How many columns?(4-16): '
    return {'COLUMNS': num(colprompt), 'ROWS': num(rowprompt),
            'tl': plch(tlprompt), 'm1': plch(m1prompt)}

    
def print_board(board: list):
    for r in board:
        row = ''
        for i in r:
            row += (i + ' ')
        print(row)

def print_score(g_thang):
    scores = g_thang.scores()
    print('Black:', scores['B'],
          'White:', scores['W'])

def get_num(prompt) ->None:
    while True:
        try:
            num = int(input(prompt)) - 1
            break
        except:
            print('Try an integer!')
    return num

def get_move(g_thang) ->None:
    print('It is ', g_thang.turn_name(), "'s move.")
    while True:
        if g_thang.poss_moves(g_thang.turn()) == []:
            print(g_thang.turn_name(), 'has no valid turn.')
            g_thang.move(0, 0, g_thang.turn())
            break
        row = get_num('Please enter the row: ')
        column = get_num('Please enter the column: ')
        try:
            g_thang.move(column, row, g_thang.turn())
            break
        except:
            print('Not a valid move')


def winner_print(g_thang) ->str:
    winner = g_thang.winner()
    if winner != othello_logic.NONE:
        print('Look at that. ', winner, ' is the winner!')
    return winner


    
    