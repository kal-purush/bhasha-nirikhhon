from random import randint
import copy
import os
from time import sleep

present = [[0]*10 for i in range(10)]
future = [[0]*10 for i in range(10)]

for i in range(20):
    x = randint(0, 9)
    y = randint(0, 9)

    while(present[x][y] != 0):
        x = randint(0, 9)
        y = randint(0, 9)

    present[x][y] = 1
    future[x][y] = 1

    for j in range(10):
        print (present[j])


def do_game():
    global present
    for i in range(1, 9):
        for j in range(1, 9):
            brkom = count_n(i, j)
            if(brkom < 2): future[i][j] = 0
            elif(brkom >= 2 and brkom <= 3 and present[i][j] == 1): present[i][j] = 1
            elif(brkom > 3): future[i][j] = 0
            elif(brkom == 3 and present[i][j] == 0): future[i][j] = 1
            present = copy.deepcopy(future)


def count_n(i, j):
    brkom = 0
    if(present[i-1][j-1] == 1): brkom += 1
    if(present[i-1][j] == 1): brkom += 1
    if(present[i-1][j+1] == 1): brkom += 1
    if(present[i+1][j-1] == 1): brkom += 1
    if(present[i+1][j] == 1): brkom += 1
    if(present[i+1][j+1] == 1): brkom += 1
    if(present[i][j-1] == 1): brkom += 1
    if(present[i][j+1] == 1): brkom += 1
    return brkom


while(1):
    do_game()
    os.system("cls")
    print ("\nFuture Map")
    for j in range(10):
        print (present[j])
    sleep(1)