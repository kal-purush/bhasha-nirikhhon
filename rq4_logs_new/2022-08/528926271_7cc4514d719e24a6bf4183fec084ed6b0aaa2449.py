# use for printing 8x8 matrix
positions=[[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8],[1,2,3,4,5,6,7,8]]

# initialise all white pieces with starting positions
whiteArmy = {'wp1':[6,0],'wp2':[6,1],'wp3':[6,2],'wp4':[6,3],'wp5':[6,4],'wp6':[6,5],'wp7':[6,6],'wp8':[6,7],
              "wr1": [7,0], "wr2": [7,7],"wk1": [7,1], "wk2": [7,6], "wb1": [7,2], "wb2": [7,5], "wking": [7,3], "wqueen": [7,4]}

# initialise all black pieces with starting positions
blackArmy = {'bp1':[1,0],'bp2':[1,1],'bp3':[1,2],'bp4':[1,3],'bp5':[1,4],'bp6':[1,5],'bp7':[1,6],'bp8':[1,7],
              'br1':[0,0],'br2':[0,7], 'bk1':[0,1],'bk2':[0,6], 'bb1':[0,2],'bb2':[0,5], 'bking':[0,3],'bqueen':[0,4]}

# makeshift objects for logic purposes
whitePawns = ['wp1','wp2','wp3','wp4','wp5','wp6','wp7','wp8']
whiteRooks = ['wr1','wr2']
whiteKnights = ['wk1','wk2']
whiteBishops = ['wb1','wb2']
whiteKing = 'wking'
whiteQueen = 'wqueen'
blackPawns = ['bp1','bp2','bp3','bp4','bp5','bp6','bp7','bp8']
blackRooks = ['br1','br2']
blackKnights = ['bk1','bk2']
blackBishops = ['bb1','bb2']
blackKing = 'bking'
blackQueen = 'bqueen'

# a compact display function
def display():
    for i in range(len(positions)):
        for j in range(8):
            if whiteArmy['wp1'] == [i,j]:
                print('wp1', end='\t')
            elif whiteArmy['wp2'] == [i,j]:
                print('wp2', end='\t')
            elif whiteArmy['wp3'] == [i,j]:
                print('wp3', end='\t')
            elif whiteArmy['wp4'] == [i,j]:
                print('wp4', end='\t')
            elif whiteArmy['wp5'] == [i,j]:
                print('wp5', end='\t')
            elif whiteArmy['wp6'] == [i,j]:
                print('wp6', end='\t')
            elif whiteArmy['wp7'] == [i,j]:
                print('wp7', end='\t')
            elif whiteArmy['wp8'] == [i,j]:
                print('wp8', end='\t')
            elif whiteArmy['wr1'] == [i, j]:
                print('wr1', end='\t')
            elif whiteArmy['wr2'] == [i, j]:
                print('wr2', end='\t')
            elif whiteArmy['wk1'] == [i, j]:
                print('wk1', end='\t')
            elif whiteArmy['wk2'] == [i, j]:
                print('wk2', end='\t')
            elif whiteArmy['wb1'] == [i, j]:
                print('wb1', end='\t')
            elif whiteArmy['wb2'] == [i, j]:
                print('wb2', end='\t')
            elif whiteArmy['wking'] == [i, j]:
                print('WK', end='\t')
            elif whiteArmy['wqueen'] == [i, j]:
                print('WQ', end='\t')
            elif blackArmy['bp1'] == [i,j]:
                print('bp1', end='\t')
            elif blackArmy['bp2'] == [i,j]:
                print('bp2', end='\t')
            elif blackArmy['bp3'] == [i,j]:
                print('bp3', end='\t')
            elif blackArmy['bp4'] == [i,j]:
                print('bp4', end='\t')
            elif blackArmy['bp5'] == [i,j]:
                print('bp5', end='\t')
            elif blackArmy['bp6'] == [i,j]:
                print('bp6', end='\t')
            elif blackArmy['bp7'] == [i,j]:
                print('bp7', end='\t')
            elif blackArmy['bp8'] == [i,j]:
                print('bp8', end='\t')
            elif blackArmy['br1'] == [i, j]:
                print('br1', end='\t')
            elif blackArmy['br2'] == [i, j]:
                print('br2', end='\t')
            elif blackArmy['bk1'] == [i, j]:
                print('bk1', end='\t')
            elif blackArmy['bk2'] == [i, j]:
                print('bk2', end='\t')
            elif blackArmy['bb1'] == [i, j]:
                print('bb1', end='\t')
            elif blackArmy['bb2'] == [i, j]:
                print('bb2', end='\t')
            elif blackArmy['bking'] == [i, j]:
                print('BK', end='\t')
            elif blackArmy['bqueen'] == [i, j]:
                print('BQ', end='\t')
            else:
                print('-', end='\t')
        print()

def move(ele):
    currentFigure = ele
    if currentFigure in whitePawns:
        checkPos = [whiteArmy[currentFigure][0]-1,whiteArmy[currentFigure][1]] # check desired move of the figure
        # check diagonals for kill 
        checkKillLeft = [whiteArmy[currentFigure][0]-1,whiteArmy[currentFigure][1]-1] 
        checkKillRight = [(whiteArmy[currentFigure][0])-1,(whiteArmy[currentFigure][1])+1]
        if checkPos[0]>=0 and checkPos not in whiteArmy.values():
            if checkKillRight in blackArmy.values():
                enemy = str({i for i in blackArmy if blackArmy[i]==checkKillRight})
                ch = input(f'you can kill {enemy}, do it? y/n')
                if ch == 'y':
                    whiteArmy[currentFigure] = checkKillRight
                    blackArmy[enemy] = None
                else:
                    whiteArmy[currentFigure][0]-=1
            elif checkKillLeft in blackArmy.values():
                enemy = str({i for i in blackArmy if blackArmy[i]==checkKillLeft})
                ch = input(f'you can kill {enemy}, do it? y/n')
                if ch == 'y':
                    whiteArmy[currentFigure] = checkKillLeft
                    blackArmy[enemy] = None
                else:
                    whiteArmy[currentFigure][0]-=1
            else:
                whiteArmy[currentFigure][0]-=1
        elif checkPos in blackArmy.values():
            if checkKillRight in blackArmy.values():
                enemy = str({i for i in blackArmy if blackArmy[i]==checkKillRight})
                ch = input(f'you can kill {enemy}, do it? y/n')
                if ch == 'y':
                    whiteArmy[currentFigure] = checkKillRight
                    blackArmy[enemy] = None
                else:
                    print('illegal move')
            elif checkKillLeft in blackArmy.values():
                enemy = str({i for i in blackArmy if blackArmy[i]==checkKillLeft})
                ch = input(f'you can kill {enemy}, do it? y/n')
                if ch == 'y':
                    whiteArmy[currentFigure] = checkKillLeft
                    blackArmy[enemy] = None
                else:
                    print('illegal move')
        else:
            print('illegal move')
    elif currentFigure in whiteRooks:
        move = input('choose vertical or horizontal v/h')
        if move == 'v':
            ch = input('up or down? u/d')
            if ch == 'u':
                dist = int(input('enter number of fields'))
                checkPos = [(whiteArmy[currentFigure][0])-dist,whiteArmy[currentFigure][1]]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos in whiteArmy.values() or checkPos[0]<0:
                    print('illegal move')
                else:
                    whiteArmy[currentFigure] = checkPos
                    print(checkPos)
            elif ch == 'd':
                dist = int(input('enter number of fields'))
                checkPos = [(whiteArmy[currentFigure][0])+dist,whiteArmy[currentFigure][1]]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[0]<=7:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            else:
                print('invalid input')
        elif move == 'h':
            ch = input('right or left r/l')
            if ch == 'r':
                dist = int(input('enter number of moves'))
                checkPos = [whiteArmy[currentFigure][0],(whiteArmy[currentFigure][1])+dist]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[1]<=7:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            elif ch == 'l':
                dist = int(input('enter number of moves'))
                checkPos = [whiteArmy[currentFigure][0],(whiteArmy[currentFigure][1])-dist]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[1]>=0:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            else:
                print('invalid input')
        else:
            print('invalid input')
    elif currentFigure in whiteBishops:
        move = input('choose up or down u/d')
        if move == 'u':
            ch = input('right or left r/l')
            if ch == 'r':
                dist = int(input('enter number of fields'))
                checkPos = [whiteArmy[currentFigure][0]-dist,(whiteArmy[currentFigure][1])+dist]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[0]>=0 and checkPos[1]<=7:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            elif ch == 'l':
                dist = int(input('enter number of fields'))
                checkPos = [whiteArmy[currentFigure][0]-dist,(whiteArmy[currentFigure][1])-dist]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[0]>=0 and checkPos[1]>=0:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            else:
                print('invalid input')
        elif move == 'd':
            ch = input('right or left r/l')
            if ch == 'r':
                dist = int(input('enter number of fields'))
                checkPos = [whiteArmy[currentFigure][0]+dist,(whiteArmy[currentFigure][1])+dist]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[0]<=7 and checkPos[1]<=7:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            elif ch == 'l':
                dist = int(input('enter number of fields'))
                checkPos = [whiteArmy[currentFigure][0]+dist,(whiteArmy[currentFigure][1])-dist]
                if checkPos in blackArmy.values():
                    enemy = str({i for i in blackArmy if blackArmy[i]==checkPos})
                    ch = input(f'you can kill {enemy}, do it? y/n (n will cancel the move)')
                    if ch == 'y':
                        whiteArmy[currentFigure] = checkPos
                        blackArmy[enemy] = None
                elif checkPos not in whiteArmy.values() and checkPos[0]<=7 and checkPos[1]>=0:
                    whiteArmy[currentFigure] = checkPos
                else:
                    print('illegal move')
            else:
                print('invalid input')
        else:
            print('invalid input')

while True:
    display()
    ch = int(input('press 1 to move'))
    if ch == 1:
        currentFigure = input('choose an element to move: ')
        move(currentFigure)
        continue