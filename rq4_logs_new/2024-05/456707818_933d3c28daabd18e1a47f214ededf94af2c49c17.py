import pygame

import sys
import random
import time


def baseMaze(maze):
    maze = [
        ["_"] * 14 + [" "] + ["_"],
        ["|"] + [" "] * 2 + ["|"] * 3 + [" "] * 3 + ["|"] * 4 + [" "] * 2 + ["|"],
        ["|"] + [" "] * 4 + ["|"] * 1 + [" "] * 2 + ["|"] * 3 + [" "] * 4 + ["|"],
        ["|"] + ["|"] * 2 + [" "] * 1 + ["|"] * 2 + [" "] * 1 + ["|"] * 2 + [" "] * 3 + ["|"] * 2 + [" "] * 1 + ["|"],
        ["|"] + ["|"] * 1 + [" "] * 2 + ["|"] * 1 + [" "] * 3 + ["|"] * 1 + [" "] * 2 + ["|"] * 2 + [" "] * 2 + ["|"],
        ["|"] + [" "] * 2 + ["|"] * 2 + [" "] * 1 + ["|"] * 2 + [" "] * 2 + ["|"] * 1 + ["|"] * 2 + [" "] * 2 + [
            "|"] * 1 + ["|"],
        ["|"] + ["|"] * 1 + [" "] * 1 + ["|"] * 1 + [" "] * 3 + ["|"] * 2 + [" "] * 2 + ["|"] * 4 + ["|"],
        ["|"] + [" "] * 3 + ["|"] * 2 + [" "] * 1 + ["|"] * 1 + [" "] * 1 + [" "] * 2 + ["|"] * 3 + [" "] * 1 + ["|"],
        ["|"] + [" "] * 1 + ["|"] * 4 + [" "] * 3 + ["|"] * 2 + ["|"] * 1 + [" "] * 3 + ["|"] * 1 + [" "] * 1 + ["|"],
        ["|"] + [" "] * 4 + ["|"] * 3 + [" "] * 1 + [" "] * 6 + ["|"],
        ["|"] + ["|"] * 3 + [" "] * 2 + ["|"] * 1 + [" "] * 2 + ["|"] * 4 + [" "] * 1 + ["|"] * 1 + ["|"],
        ["|"] + [" "] * 4 + ["|"] * 2 + [" "] * 2 + [" "] * 1 + ["|"] * 2 + [" "] * 2 + ["|"] * 1 + ["|"],
        ["|"] + [" "] * 1 + ["|"] * 1 + [" "] * 1 + ["|"] * 2 + [" "] * 5 + ["|"] * 1 + [" "] * 3 + ["|"],
        ["|"] + [" "] * 2 + ["|"] * 1 + [" "] * 2 + ["|"] * 2 + [" "] * 4 + ["|"] * 2 + [" "] * 1 + ["|"],
        ["|"] + [" "] * 2 + ["|"] * 2 + [" "] * 1 + [" "] * 3 + ["|"] * 2 + [" "] * 3 + [" "] * 1 + ["|"],
        ["|"] + [" "] * 9 + ["|"] * 1 + [" "] * 4 + ["|"],
        ["|"] * 7 + ["Ω"] + ["|"] * 8,
        ["_"] * 16
    ]
    return maze


def baseMazePrint(printmaze):
    cell_size = width // len(printmaze[0]), height // len(printmaze)

    for i in range(len(printmaze)):
        for j in range(len(printmaze[0])):
            rect = pygame.Rect(j * cell_size[0], i * cell_size[1], cell_size[0], cell_size[1])
            if printmaze[i][j] == "_":
                pygame.draw.rect(screen, (255, 255, 255), rect)
            elif printmaze[i][j] == "|":
                pygame.draw.rect(screen, (255, 255, 255), rect)
            elif printmaze[i][j] == "Ω":
                pygame.draw.rect(screen, (0, 0, 0), rect)
            elif printmaze[i][j] == " ":
                pygame.draw.rect(screen, (0, 0, 0), rect)
    pygame.display.update()


def printMaze(printmaze):
    cell_size = width // len(printmaze[0]), height // len(printmaze)

    for i in range(len(printmaze)):
        for j in range(len(printmaze[0])):
            rect = pygame.Rect(j * cell_size[0], i * cell_size[1], cell_size[0], cell_size[1])
            if printmaze[i][j] == "_":
                pygame.draw.rect(screen, (255, 255, 255), rect)
            elif printmaze[i][j] == "|":
                pygame.draw.rect(screen, (255, 255, 255), rect)
            elif printmaze[i][j] == "Ω":
                pygame.draw.rect(screen, (0, 255, 0), rect)
    pygame.display.update()


def addingMoves(lst):
    lst.append(random.randint(1, 4))
    lst.append(random.randint(1, 4))


def rowAdder(row, col, underScoreadd, addmaze):
    if not underScoreadd:
        addmaze[row][col] = "_"
    else:
        addmaze[row][col] = " "


def moving(guy, rowcol, underScoreChange, amount, playmaze):
    score = 0
    baseMazePrint(maze)
    time.sleep(0.001)
    for i in range(amount):
        row = rowcol[0]
        col = rowcol[1]
        movement = guy[i]

        if (movement == 1) and (playmaze[row - 1][col] != "_") and (playmaze[row - 1][col] != "|"):
            # up move
            rowAdder(row, col, underScoreChange, playmaze)
            playmaze[row - 1][col] = "Ω"
            rowcol[0] = rowcol[0] - 1
            underScoreChange = True
            if (row == 1) and (col == 14):
                baseMazePrint(maze)
                printMaze(playmaze)
                print("You win!")
                global gameOn
                gameOn = False
            else:
                score += 4
                printMaze(maze)
                time.sleep(0.05)
        elif (movement == 2) and (playmaze[row][col - 1] != "|"):
            # left move
            rowAdder(row, col, underScoreChange, playmaze)
            if playmaze[row][col - 1] == "_":
                underScoreChange = False
            else:
                underScoreChange = True
            playmaze[row][col - 1] = "Ω"
            rowcol[1] = rowcol[1] - 1
            score += 2
            printMaze(maze)
            time.sleep(0.05)
        elif (movement == 3) and (playmaze[row][col + 1] != "|"):
            # right move
            rowAdder(row, col, underScoreChange, playmaze)
            if playmaze[row][col + 1] == "_":
                underScoreChange = False
            else:
                underScoreChange = True
            playmaze[row][col + 1] = "Ω"
            rowcol[1] = rowcol[1] + 1
            score += 3
            printMaze(maze)
            time.sleep(0.05)
        elif (movement == 4) and (underScoreChange == True) and (playmaze[row + 1][col] != "|"):
            # down move
            if row == 16:
                score -= 1000
                return score
            elif playmaze[row + 1][col] == "_":
                underScoreChange = False
            else:
                underScoreChange = True
            playmaze[row + 1][col] = "Ω"
            rowcol[0] = rowcol[0] + 1
            score += 1
            printMaze(maze)
            time.sleep(0.05)
        else:
            score -= 1000
            return score

    return score


# Pygame initialization
pygame.init()
width, height = 800, 800
screen = pygame.display.set_mode((width, height))
pygame.display.set_caption("Maze Solver")

clock = pygame.time.Clock()
FPS = 60

gameOn = True
underScorelist = [True] * 32
amount = 0
maze = []
guylist = [[] for _ in range(32)]
scorelist = [0] * 32
generaltrack = 0
tStart = time.time()
while gameOn:
    generaltrack += 2
    amount += 2

    for _ in range(32):
        maze = baseMaze(maze)
        rowcol = [16, 7]  # Starting position adjusted to the middle of the bottom row
        addingMoves(guylist[_])
        scorelist[_] += moving(guylist[_], rowcol, underScorelist[_], amount, maze)

        if not gameOn:
            break

    if not gameOn:
        print(amount)
        break

    max_index = scorelist.index(max(scorelist))

    if max(scorelist) < 0:
        generaltrack -= 2
        for i in range(8):
            guylist[i].pop()
            guylist[i].pop()
    else:
        for i in range(len(guylist)):
            if i != max_index:
                for j in range(generaltrack):
                    guylist[i][j] = guylist[max_index][j]

        for i in range(int(len(guylist) / 2)):
            for j in range(generaltrack):
                temp = random.randint(1, int(generaltrack / 2))
                if temp == 1:
                    guylist[i][j] = random.randint(1, 4)

        for i in range(len(scorelist)):
            if i != max_index:
                scorelist[i] = scorelist[max_index]
tEnd = time.time() - tStart
print(tEnd)