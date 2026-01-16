import copy
import time
import random

# Objectives
def inverse(move):
    if move == "R":
        ret = "Rp"
    if move == "Rp":
        ret = "R"
    if move == "L":
        ret = "Lp"
    if move == "Lp":
        ret = "L"
    if move == "D":
        ret = "Dp"
    if move == "Dp":
        ret = "D"
    if move == "B":
        ret = "Bp"
    if move == "Bp":
        ret = "B"
    return ret

def move_R(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    # Takes a Permute object as an input and produces the permute object after applying R move on it
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    F[0] = permF[0]
    F[1] = permD[0]
    F[2] = permD[1]
    F[3] = permD[2]
    F[4] = permF[4]
    F[5] = permF[5]

    R[0] = permR[0]
    R[1] = permR[1]
    R[2] = permR[2]
    R[3] = permF[1]
    R[4] = permF[2]
    R[5] = permF[3]

    L = copy.deepcopy(permL)

    D[0] = permR[3]
    D[1] = permR[4]
    D[2] = permR[5]
    D[3] = permD[3]
    D[4] = permD[4]
    D[5] = permD[5]

    return "".join(F) + "".join(R) + "".join(L) + "".join(D)
def move_Rp(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    F[0] = permF[0]
    F[1] = permR[3]
    F[2] = permR[4]
    F[3] = permR[5]
    F[4] = permF[4]
    F[5] = permF[5]

    R[0] = permR[0]
    R[1] = permR[1]
    R[2] = permR[2]
    R[3] = permD[0]
    R[4] = permD[1]
    R[5] = permD[2]

    L = copy.deepcopy(permL)

    D[0] = permF[1]
    D[1] = permF[2]
    D[2] = permF[3]
    D[3] = permD[3]
    D[4] = permD[4]
    D[5] = permD[5]

    return "".join(F) + "".join(R) + "".join(L) + "".join(D)

def move_L(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    F[0] = permF[0]
    F[1] = permF[1]
    F[2] = permF[2]
    F[3] = permL[1]
    F[4] = permL[2]
    F[5] = permL[3]


    L[0] = permL[0]
    L[1] = permD[4]
    L[2] = permD[5]
    L[3] = permD[0]
    L[4] = permL[4]
    L[5] = permL[5]

    D[0] = permF[5]
    D[1] = permD[1]
    D[2] = permD[2]
    D[3] = permD[3]
    D[4] = permF[3]
    D[5] = permF[4]

    R = copy.deepcopy(permR)

    return "".join(F) + "".join(R) + "".join(L) + "".join(D)
def move_Lp(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    F[0] = permF[0]
    F[1] = permF[1]
    F[2] = permF[2]
    F[3] = permD[4]
    F[4] = permD[5]
    F[5] = permD[0]


    L[0] = permL[0]
    L[1] = permF[3]
    L[2] = permF[4]
    L[3] = permF[5]
    L[4] = permL[4]
    L[5] = permL[5]

    D[0] = permL[3]
    D[1] = permD[1]
    D[2] = permD[2]
    D[3] = permD[3]
    D[4] = permL[1]
    D[5] = permL[2]

    R = copy.deepcopy(permR)

    return "".join(F) + "".join(R) + "".join(L) + "".join(D)

def move_B(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    R[0] = permR[0]
    R[1] = permD[2]
    R[2] = permD[3]
    R[3] = permD[4]
    R[4] = permR[4]
    R[5] = permR[5]


    L[0] = permL[0]
    L[1] = permL[1]
    L[2] = permL[2]
    L[3] = permR[1]
    L[4] = permR[2]
    L[5] = permR[3]

    D[0] = permD[0]
    D[1] = permD[1]
    D[2] = permL[3]
    D[3] = permL[4]
    D[4] = permL[5]
    D[5] = permD[5]

    F = copy.deepcopy(permF)

    return "".join(F) + "".join(R) + "".join(L) + "".join(D)
def move_Bp(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    R[0] = permR[0]
    R[1] = permL[3]
    R[2] = permL[4]
    R[3] = permL[5]
    R[4] = permR[4]
    R[5] = permR[5]


    L[0] = permL[0]
    L[1] = permL[1]
    L[2] = permL[2]
    L[3] = permD[2]
    L[4] = permD[3]
    L[5] = permD[4]

    D[0] = permD[0]
    D[1] = permD[1]
    D[2] = permR[1]
    D[3] = permR[2]
    D[4] = permR[3]
    D[5] = permD[5]

    F = copy.deepcopy(permF)

    return "".join(F) + "".join(R) + "".join(L) + "".join(D)

def move_D(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])
    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    R[0] = permR[0]
    R[1] = permR[1]
    R[2] = permF[2]
    R[3] = permF[3]
    R[4] = permF[4]
    R[5] = permR[5]


    L[0] = permL[0]
    L[1] = permL[1]
    L[2] = permR[2]
    L[3] = permR[3]
    L[4] = permR[4]
    L[5] = permL[5]

    F[0] = permF[0]
    F[1] = permF[1]
    F[2] = permL[2]
    F[3] = permL[3]
    F[4] = permL[4]
    F[5] = permF[5]

    D[0] = permD[4]
    D[1] = permD[5]
    D[2] = permD[0]
    D[3] = permD[1]
    D[4] = permD[2]
    D[5] = permD[3]


    return "".join(F) + "".join(R) + "".join(L) + "".join(D)
def move_Dp(perm):
    permF = list(perm[0:6])
    permR = list(perm[6:12])
    permL = list(perm[12:18])
    permD = list(perm[18:24])

    F = [None,None,None,None,None,None]
    R = [None,None,None,None,None,None]
    L = [None,None,None,None,None,None]
    D = [None,None,None,None,None,None]

    R[0] = permR[0]
    R[1] = permR[1]
    R[2] = permL[2]
    R[3] = permL[3]
    R[4] = permL[4]
    R[5] = permR[5]


    L[0] = permL[0]
    L[1] = permL[1]
    L[2] = permF[2]
    L[3] = permF[3]
    L[4] = permF[4]
    L[5] = permL[5]

    F[0] = permF[0]
    F[1] = permF[1]
    F[2] = permR[2]
    F[3] = permR[3]
    F[4] = permR[4]
    F[5] = permF[5]

    D[0] = permD[2]
    D[1] = permD[3]
    D[2] = permD[4]
    D[3] = permD[5]
    D[4] = permD[0]
    D[5] = permD[1]


    return "".join(F) + "".join(R) + "".join(L) + "".join(D)


MOVE_LIST = {
    "R": move_R,
    "Rp": move_Rp,
    "L": move_L,
    "Lp": move_Lp,
    "D": move_D,
    "Dp": move_Dp,
    "B": move_B,
    "Bp": move_Bp
}



def solve():
    start = "YYYYYYBBBBBBGGGGGGRRRRRR"
    opened = {start : [0,""]} # store length of the path and the path as a string of moves that solves the given permutation
    closed = {}# 
    count = 0
    stamp = time.time()
    fout = open('../data/permutations.txt','w')
    
    while opened:
        for current in opened:
            break
        count += 1
        cost = opened[current][0]
        path = opened[current][1]
        closed[current] = opened.pop(current)
        fout.write(f'{current}: {closed[current][0]} {closed[current][1]}\n' )
        #if current == target:
        #    break
        print(count,time.time() - stamp)
        for move in MOVE_LIST:
            neighbour = MOVE_LIST[move](current)
            if neighbour in closed:
                pass
            else:
                if neighbour in opened and cost + 1 < opened[neighbour][0]:
                    opened[neighbour][0] = cost + 1
                    opened[neighbour][1] = inverse(move) + path
                if not neighbour in opened:
                    opened[neighbour] = [cost+1, inverse(move)+" " + path]
                    opened[neighbour][0] = cost + 1
    fout.close()
    return "Done"
solve()