import numpy as np

size = 15

def move2xy(move):
    return move % size, int(move / size)

def xy2move(x, y):
    return y * size + x

def get_xy(board, x, y):
    if x >=0 and y >= 0 and x < size and y < size:
        return board[y*size + x]

_5 = 1
_4X4_WITH_BLOCK = 2
_4X3_WITHOUT_BLOCK = 3
_4X3_WITH_BLOCK = 4
_4_WITHOUT_BLOCK = 5
_3X3_WIHTOUT_BLOCK = 9


_2X3_WITHOUT_BLOCK = 10
_2X2X2_WITHOUT_BLOCK = 11
_3X3_WIHT_HALFBLOCK = 12
_4_WITH_BLOCK = 13
_3X3_WIHT_BLOCK = 14
_2X2_WITHOUT_BLOCK = 17
_3_WITHOUT_BLOCK = 18
_3_WITH_BLOCK = 19
_2_WITHOUT_BLOCK = 20
_2_WITH_BLOCK = 21

SCORES = dict()

SCORES[_5] = 100
SCORES[_4X4_WITH_BLOCK] = 100
SCORES[_4X3_WITHOUT_BLOCK] = 100
SCORES[_4X3_WITH_BLOCK] = 100
SCORES[_4_WITHOUT_BLOCK] = 100
SCORES[_3X3_WIHTOUT_BLOCK] = 100
SCORES[_2X3_WITHOUT_BLOCK] = 21
SCORES[_2X2X2_WITHOUT_BLOCK] = 20
SCORES[_3X3_WIHT_HALFBLOCK] = 10
SCORES[_4_WITH_BLOCK] = 9
SCORES[_3X3_WIHT_BLOCK] = 8
SCORES[_2X2_WITHOUT_BLOCK] = 5
SCORES[_3_WITHOUT_BLOCK] = 4
SCORES[_3_WITH_BLOCK] = 3
SCORES[_2_WITHOUT_BLOCK] = 2
SCORES[_2_WITH_BLOCK] = 1


def evaluate(board, move):

    patterns = []
    
    delta = [(1,0), (0,1), (1,1), (1,-1)]
    for d in delta:
        dx = d[0]
        dy = d[1]

        x, y = move2xy(move)
        count = 0
        open = 0
        
        # 연속된 돌의 시작 부분을 찾는다
        while get_xy(board, x-dx, y-dy) == 1: 
            x-=dx
            y-=dy
        
        # 시작 돌이 열려있는지 (옆에 다른 돌이 막거나 길의 끝인지) 확인한다
        if get_xy(board, x-dx, y-dy) == 0:
            open += 1

        #  연속된 돌의 수를 센다
        while get_xy(board, x, y) == 1:
            count+=1
            x+=dx
            y+=dy

        # 다른 한쪽도 열려있는지 확인한다
        if get_xy(board, x, y) == 0:
            open += 1

        
        

     
        # 맞는 패턴을 부여한다
        if count == 5:
            patterns.append(_5) # 오목
        elif count == 4 and open == 2:
            patterns.append(_4_WITHOUT_BLOCK) # 방어없는 사목
        elif count == 4 and open == 1:
            patterns.append(_4_WITH_BLOCK) # 한쪽이 방어된 사목
        elif count == 3 and open == 2:
            patterns.append(_3_WITHOUT_BLOCK) # 방어없는 삼목
        elif count == 3 and open == 1:
            patterns.append(_3_WITH_BLOCK) # 한쪽이 방어된 삼목
        elif count == 2 and open == 2:
            patterns.append(_2_WITHOUT_BLOCK) # 방어없는 이목
        elif count == 2 and open == 1:
            patterns.append(_2_WITH_BLOCK) # 한쪽이 방어된 이목
    
    # 조합을 만든다 
    cnt_2stone = patterns.count(_2_WITHOUT_BLOCK)
    if cnt_2stone == 2:
        patterns.append(_2X2_WITHOUT_BLOCK) # 방어없는 이이
    elif cnt_2stone >= 3:
        patterns.append(_2X2X2_WITHOUT_BLOCK) # 방어없는 이이이
    
    if patterns.count(_3_WITHOUT_BLOCK) >= 2:
        patterns.append(_3X3_WIHTOUT_BLOCK) # 방어없는 삼삼

    if patterns.count(_3_WITH_BLOCK) >= 2:
        patterns.append(_3X3_WIHT_BLOCK) # 양쪽이 다 방어있는 삼삼

    if (_3_WITHOUT_BLOCK in patterns) and (_3_WITH_BLOCK in patterns): 
        patterns.append(_3X3_WIHT_HALFBLOCK) # 한쪽만 방어있는 삼삼

    if (_3_WITHOUT_BLOCK in patterns) and (_2_WITHOUT_BLOCK in patterns):
        patterns.append(_2X3_WITHOUT_BLOCK) # 방어없는 이삼

    if (_4_WITH_BLOCK in patterns) and (_3_WITHOUT_BLOCK in patterns):
        patterns.append(_4X3_WITHOUT_BLOCK) # 방어없는 사삼

    if (_4_WITH_BLOCK in patterns) and (_3_WITH_BLOCK in patterns):
        patterns.append(_4X3_WITH_BLOCK) # 방어된 사삼

    if patterns.count(_4_WITH_BLOCK) >= 2:
        patterns.append(_4X4_WITH_BLOCK) # 방어된 사사

    #  조합에 따른 점수를 부여한다
    best = 0
    for pattern in patterns:
        value = SCORES[pattern]
        if value > best:
            best = value

    return best


if __name__ == '__main__':
    board = np.zeros(size*size)
    board[1] = 1
    board[2] = 1

    print(evaluate(board, 2))
    board[0] = 2
    print(evaluate(board, 2))

    board[13] = 1
    board[28] = 1
    board[43] = 1
    board[58] = 1
    print(evaluate(board, 58))
    
    board[13] = 0
    board[73] = 1
    print(evaluate(board, 58))


    board[111] = 1
    board[112] = 1
    board[125] = 1
    board[95] = 1
    print(evaluate(board, 111))

