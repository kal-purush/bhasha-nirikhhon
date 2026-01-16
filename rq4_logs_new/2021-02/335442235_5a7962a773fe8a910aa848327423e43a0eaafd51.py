#HuntTheWumpus.py
#Wumpus World driver
#COSC370, Project 1
#Alan C. Jamieson
#Latest Revision: February 4, 2021

#This driver will ask the user for some information in regards to the format of the Hunt the Wumpus game (credit: Gregory Yob).
#This information will then be passed to the WumpusAgent module (user provided), then randomly assign wumpi, pits, and gold.
#The driver will simulate and provide sensory data as specified in the project document.

#Note: there are very few self-error checks as part of this program.
from random import randint
import WumpusAgent
from datetime import datetime
import time
import sys
#--------------------------
#globals
#--------------------------

#player location
playerx = 0
playery = 0

#player got gold?
gotgold = False

#wumpus two dimensional array. first value is x, second value is y
wumpilist = []

#--------------------------
#game functionalities
#--------------------------

#setupBoard - takes number of wumpi and arrows from input, and returns a randomly sized, populated board
#Key:
#g = gold
#e = player entry
#p = pit
#w = wumpus
#0 = empty space
def setupBoard(wumpi, arrows):
    #get size of board, needs to be at least the number of wumpi + 2 * 2
    n = randint(wumpi+2, 200)

    #initialize board, 0 represents a blank space
    l = []
    for i in range(n):
        temp = [0] * n
        l.append(temp)

    #place the gold
    gx = randint(0, n-1)
    gy = randint(0, n-1)
    l[gx][gy] = 'g'

    #populate the pits, avoiding the gold, capped at twice the size of a single dimension
    #note: we are secretly ok with a pit on top of a pit, so we don't check for that
    numpits = randint(0, n*2)
    for i in range(numpits):
        x = randint(0, n-1)
        y = randint(0, n-1)
        while(x == gx and y == gy):
            x = randint(0, n-1)
            y = randint(0, n-1)
        l[x][y] = 'p'

    #populate the wumpi, avoiding gold and pits
    for i in range(wumpi):
        #loop for checking our x, y value
        flag = True
        while(flag):
            x = randint(0, n-1)
            y = randint(0, n-1)
            if l[x][y] == 0:
                l[x][y] = 'w'
                wumpilist.append([x,y])
                flag = False

    #place the entrance, updating player x and y value
    flag = True
    while(flag):
        x = randint(0, n-1)
        y = randint(0, n-1)
        if l[x][y] == 0:
            l[x][y] = 'e'
            playerx = x
            playery = y
            flag = False

    return l, playerx, playery

#stenchCheck - takes the x and y coordinates of the player and returns if there's a wumpus in any adjacent square
def stenchCheck(x, y, l):
    if x > 0 and l[x-1][y] == 'w':
        return True
    if x < len(l)-1 and l[x+1][y] == 'w':
        return True
    if y > 0 and l[x][y-1] == 'w':
        return True
    if y < len(l)-1 and l[x][y+1] == 'w':
        return True
    return False

#glitterCheck - is the gold at my feet?
def glitterCheck(x, y, l):
    if l[x][y] == 'g':
        return True
    return False

#bumpCheck - check the border of the dungeon
def bumpCheck(x, y, l):
    if x < 0 or x > len(l)-1:
        return True
    if y < 0 or y > len(l)-1:
        return True
    return False

#breezeCheck - check to see if a pit is in an adjacent square
def breezeCheck(x, y, l):
    if x > 0 and l[x-1][y] == 'p':
        return True
    if x < len(l)-1 and l[x+1][y] == 'p':
        return True
    if y > 0 and l[x][y-1] == 'p':
        return True
    if y < len(l)-1 and l[x][y+1] == 'p':
        return True
    return False

#screamCheck - takes in the coordinates of the player, and the direction of the shot, returns True if a hit
def screamCheck(x, y, l, d):
    if d == 'n':
        for i in range(x, -1, -1):
            if l[i][y] == 'w':
                l[i][y] = 0
                killWumpus(i, y, l)
                return True
    if d == 's':
        for i in range(x, len(l)):
            if l[i][y] == 'w':
                l[i][y] = 0
                killWumpus(i, y, l)
                return True
    if d == 'e':
        for i in range(y, len(l)):
            if l[x][i] == 'w':
                l[x][i] = 0
                killWumpus(x, i, l)
                return True
    if d == 'w':
        for i in range(y, -1, -1):
            if l[x][i] == 'w':
                l[x][i] = 0
                killWumpus(x, i, l)
                return True
    return False

#kill the wumpus at x, y by removing it from the list
def killWumpus(x, y, l):
    for i in wumpilist:
        if i[0] == x and i[1] == y:
            wumpilist.remove([x, y])
            break

#deathCheck - takes in the coordinates of the player, and determines if the player got eaten or fell into a pit
def deathCheck(x, y, l):
    if l[x][y] == 'w' or l[x][y] == 'p':
        return True
    return False

#moveWumpi - moves the wumpi, stored as a list of lists
def moveWumpi(w, l):
    #Wumpus will not move onto the gold, entrance, or onto a pit, but could be multiple wumpi in a space
    for wumpus in w:
        direction = randint(1,4)
        if direction == 1 and wumpus[0] != 0 and l[wumpus[0]-1][wumpus[1]] != 'g' and l[wumpus[0]-1][wumpus[1]] != 'p' and l[wumpus[0]-1][wumpus[1]] != 'e':
            l[wumpus[0]][wumpus[1]] = 0
            wumpus[0] = wumpus[0] - 1
            l[wumpus[0]][wumpus[1]] = 'w'
        if direction == 2 and wumpus[0] != len(l)-1 and l[wumpus[0]+1][wumpus[1]] != 'g' and l[wumpus[0]+1][wumpus[1]] != 'p' and l[wumpus[0]+1][wumpus[1]] != 'e':
            l[wumpus[0]][wumpus[1]] = 0
            wumpus[0] = wumpus[0] + 1
            l[wumpus[0]][wumpus[1]] = 'w'
        if direction == 3 and wumpus[1] != 0 and l[wumpus[0]][wumpus[1]-1] != 'g' and l[wumpus[0]][wumpus[1]-1] != 'p' and l[wumpus[0]][wumpus[1]-1] != 'e':
            l[wumpus[0]][wumpus[1]] = 0
            wumpus[1] = wumpus[1] - 1
            l[wumpus[0]][wumpus[1]] = 'w'
        if direction == 4 and wumpus[1] !=len(l)-1 and l[wumpus[0]][wumpus[1]+1] != 'g' and l[wumpus[0]][wumpus[1]+1] != 'p' and l[wumpus[0]][wumpus[1]+1] != 'e':
            l[wumpus[0]][wumpus[1]] = 0
            wumpus[1] = wumpus[1] + 1
            l[wumpus[0]][wumpus[1]] = 'w'

#winCheck - check to see if player is back on entrance tile, with the gold
def winCheck(x, y, l):
    if gotgold and l[x][y] == 'e':
        return True
    return False

#--------------------------
#driver routine starts here
#--------------------------

#get user input for type, arrows, number of wumpi, and how many games to run using these parameters
#NOTE: games will run for a maximum of 20000 agent moves
#SECOND NOTE: no user input checks are done - because Alan is lazy
#0-name
#1-type
#2-numWum
#3-numArrow
#4-numGame
print(f"Arguments count: {len(sys.argv)}")
for i, arg in enumerate(sys.argv):
    print(f"Argument {i:>6}: {arg}")
targetDir = "./AutomatedResults/"

now = datetime.now()
gametype = int(sys.argv[1])
numwumpi = int(sys.argv[2])
numarrows = int(sys.argv[3])
numgames = int(sys.argv[4])
prefix = "Moving/mvng" if gametype==2 else "Still/stll"
name = targetDir+prefix + now.strftime("%d%m%y_%H%M%S")+str(time.time())[11:15]+".txt"

#variables for stats
numwins = 0
numpitdeaths = 0
numwumpusdeaths = 0
numtimeouts = 0

#simulation start
for game in range(numgames):
    remainingarrows = numarrows
    nummoves = 0

    #set board, and player values
    board, playerx, playery = setupBoard(numwumpi, numarrows)

    #set parameter for player - this is the reset for the WumpusAgent
    WumpusAgent.setParams(gametype, numarrows, numwumpi)

    #get initial percept string
    percept = ''
    if stenchCheck(playerx, playery, board):
        percept = percept + 'S'
    if glitterCheck(playerx, playery, board):
        percept = percept + 'G'
    if breezeCheck(playerx, playery, board):
        percept = percept + 'B'
    #while the player is not dead, and hasn't won yet, get the next move
    while deathCheck != True and winCheck != True and nummoves != 4000000:
        nummoves = nummoves + 1
        #get move from agent
        move = WumpusAgent.getMove(percept)
        #intialize percept string
        percept = ''
        #move parser
        #increment based on move, perform bump checks, move back if True
        if move == 'N':
            playerx = playerx - 1
            if bumpCheck(playerx, playery, board):
                percept = percept + 'U'
                playerx = playerx + 1
        elif move == 'S':
            playerx = playerx + 1
            if bumpCheck(playerx, playery, board):
                percept = percept + 'U'
                playerx = playerx - 1
        elif move == 'E':
            playery = playery + 1
            if bumpCheck(playerx, playery, board):
                percept = percept + 'U'
                playery = playery - 1
        elif move == 'W':
            playery = playery - 1
            if bumpCheck(playerx, playery, board):
                percept = percept + 'U'
                playery = playery + 1
        #collect scream percepts
        elif move == 'SN':
            if remainingarrows > 0 and screamCheck(playerx, playery, board, 'n'):
                percept = percept + 'C'
            remainingarrows = remainingarrows - 1
        elif move == 'SS':
            if remainingarrows > 0 and screamCheck(playerx, playery, board, 's'):
                percept = percept + 'C'
            remainingarrows = remainingarrows - 1
        elif move == 'SE':
            if remainingarrows > 0 and screamCheck(playerx, playery, board, 'e'):
                percept = percept + 'C'
            remainingarrows = remainingarrows - 1
        elif move == 'SW':
            if remainingarrows > 0 and screamCheck(playerx, playery, board, 'w'):
                percept = percept + 'C'
            remainingarrows = remainingarrows - 1
        #win check
        elif move == 'C':
            if winCheck(playerx, playery, board):
                numwins = numwins + 1
                break
        elif move == 'G':
            if board[playerx][playery] == 'g':
                gotgold = True
                board[playerx][playery] = 0
        else:
            print("Incorrect move string encountered, exiting")
            exit()

        #death check!
        if deathCheck(playerx, playery, board):
            if board[playerx][playery] == 'p':
                numpitdeaths = numpitdeaths + 1
            else:
                numwumpusdeaths = numwumpusdeaths + 1
            break

        #other percepts
        if stenchCheck(playerx, playery, board):
            percept = percept + 'S'
        if glitterCheck(playerx, playery, board):
            percept = percept + 'G'
        if breezeCheck(playerx, playery, board):
            percept = percept + 'B'

        #move the wumpi if that game mode selected
        if gametype == 2:
            moveWumpi(wumpilist, board)

        #check if we timed out
        if nummoves == 4000000:
            numtimeouts = numtimeouts + 1
    #quick status print
    print("Game number " + str(game) + " complete in " + str(nummoves) + " moves.")
    #cleanup for restart
    gotgold = False
    wumpilist = []


f = open(name, "a")
f.write("games:" + str(numgames)+"\n")
f.write("wins:" + str(numwins)+"\n")
f.write("deaths:" + str(numpitdeaths + numwumpusdeaths)+"\n")
f.write("pit:" + str(numpitdeaths)+"\n")
f.write("wum:" + str(numwumpusdeaths)+"\n")
f.write("timeout:" + str(numtimeouts)+"\n")
f.close()