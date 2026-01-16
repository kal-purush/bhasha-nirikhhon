import numpy as np
import random
import time
import copy
from heapq import *

Bot = 37
Alien = 41
Crew = 49

def print_ship():
    print(ship)
    # print()
    return

def import_ship(loc):
    ship = np.loadtxt(str(loc), dtype = int)
    global dim
    dim = ship.shape[0]
    print(dim)
    ship = np.matrix(ship)
    print("Ship Layout Imported")
    return ship

def place_pieces(bot_id, alien_id, crew_id, k):
    
    open_cells = np.where(ship[:,:] == 1)
    open_cells = list(zip(list(open_cells[0]),list(open_cells[1])))
    piece_pos = random.sample(open_cells, k+2)

    global bot_pos
    bot_pos = piece_pos.pop(0)
    ship[bot_pos[0],bot_pos[1]] = bot_id
    print("Bot Placed")

    global crew_pos
    crew_pos = piece_pos.pop(0)
    ship[crew_pos[0],crew_pos[1]] = crew_id
    print("Crew Placed")

    global alien_pos
    for i in piece_pos:
        ship[i[0],i[1]] = alien_id
    print("Aliens Placed")
    alien_pos = piece_pos
    
    return

def replace_crew(prev_res):
    global crew_pos, bot_pos
    if prev_res == -1:
        ship[crew_pos[0],crew_pos[1]] = 1
    else:
        ship[crew_pos[0],crew_pos[1]] = Bot
        ship[bot_pos[0],bot_pos[1]] = 1 
        bot_pos = tuple(crew_pos)


    while True:
        crew_pos = tuple(np.random.randint(dim,size = 2))
        if ship[crew_pos[0], crew_pos[1]] == 0 or crew_pos in alien_pos or crew_pos in [bot_pos]:
            continue
        else:
            ship[crew_pos[0], crew_pos[1]] = Crew
            return


def valid_alien_moves(pos_t):
    global dim
    d = dim
    posX, posY = pos_t
    valid_list = []
    if posX+1 < d and ship[posX+1,posY] != Alien and ship[posX+1,posY] != 0:
        valid_list.append([posX+1,posY])
    if posX-1 >=0 and ship[posX-1,posY] != Alien and ship[posX-1,posY] != 0:
        valid_list.append([posX-1,posY])
    if posY+1 < d and ship[posX,posY+1] != Alien and ship[posX,posY+1] != 0:
        valid_list.append([posX,posY+1])
    if posY-1 >=0 and ship[posX,posY-1] != Alien and ship[posX,posY-1] != 0:
        valid_list.append([posX,posY-1])
    valid_list.append([posX,posY])
    return valid_list

def move_aliens():
    global alien_pos, bot_pos, crew_pos
    random.shuffle(alien_pos)
    for i in range(len(alien_pos)):
        val_moves = valid_alien_moves(alien_pos[i])
        if not len(val_moves):
            continue
        new_alien_pos = (random.choice(val_moves))
        if tuple(new_alien_pos) in [bot_pos]:
            print("Byeeeeeeeeeeee")
            exit(0)
        if tuple(alien_pos[i]) not in [crew_pos]:
            ship[alien_pos[i][0], alien_pos[i][1]] = 1
        if tuple(new_alien_pos) not in [crew_pos]:
            ship[new_alien_pos[0],new_alien_pos[1]] = Alien
        alien_pos[i] = new_alien_pos


def valid_neighbours(posX,posY):
    global dim
    d = dim
    valid_list = []
    if posX+1 < d and ship[posX+1,posY] in [1,49]:
        valid_list.append([posX+1,posY])
    if posX-1 >=0 and ship[posX-1,posY] in [1,49]:
        valid_list.append([posX-1,posY])
    if posY+1 < d and ship[posX,posY+1] in [1,49]:
        valid_list.append([posX,posY+1])
    if posY-1 >=0 and ship[posX,posY-1] in [1,49]:
        valid_list.append([posX,posY-1])
    return valid_list


def manhattan_dist(posX,posY):
    global crew_pos
    return abs(crew_pos[0]-posX) + abs(crew_pos[1]-posY)


def path_trace_algo(parents, curr_cell):
    global bot_pos
    path = []
    posX,posY = curr_cell
    while posX != -1 and posY != -1:
        path.append((posX,posY))
        posX,posY = parents[posX][posY][:2]
    return path[::-1]

def A_star_algo():
    global bot_pos, crew_pos
    print(bot_pos)
    print(crew_pos)
    visited = []
    parents = [[(-1,-1,0) for i in range(dim)] for i in range(dim)]
    heap = []
    heappush(heap, (0, bot_pos))
    visited.append(bot_pos)
    while heap:
        curr_cell = heappop(heap)
        print(curr_cell[1])
        if curr_cell[1] in [crew_pos]:
            return path_trace_algo(parents,curr_cell[1])
        valid_next_cells = valid_neighbours(curr_cell[1][0],curr_cell[1][1])
        for cell in valid_next_cells:
            if cell in visited:
                continue
            cell_heuristic = manhattan_dist(cell[0],cell[1])
            cell_g = parents[curr_cell[1][0]][curr_cell[1][1]][-1] + 1
            heappush(heap,(cell_heuristic + cell_g, tuple(cell)))
            parents[cell[0]][cell[1]] = (curr_cell[1][0], curr_cell[1][1], cell_g)
            
            for i in range(0,dim):
                for j in range(0,dim):
                    print(str(parents[i][j]), end=" ")
                print("\n")
            print("\n\n")

            visited.append(cell)
    print("No path is possible")





ship = import_ship("ship_test.txt") 
place_pieces(Bot, Alien, Crew, 3)
print_ship()
print(A_star_algo())
# backup = ship.shape[0] + 1
# while True:
#     time.sleep(1)
#     print("\033[A" * backup)
#     move_aliens()
#     print_ship()







