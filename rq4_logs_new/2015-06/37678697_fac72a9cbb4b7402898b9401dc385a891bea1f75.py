__author__ = 'travis'

import time
import sys
import random
import collections

class Node:
    def __init__(self):
        self.player_state = [] # board of 0, 1, 2 : 0 = none, 1 = player1, 2 = player2.
        # self.action = () # Coord to plop

    def leaf(self):
        for row in self.player_state:
            for item in row:
                if item == 0:
                    return False

        return True

    def print_state(self):
        for row in self.player_state:
            print row


class Wargame:
    def __init__(self, filename):
        self.board = []
        self.dimensions = ()
        self._readfile(filename)

    def _readfile(self, filename):
        fileptr = open(filename, 'r')
        temp_row = []
        for line in fileptr:
            temp_row = line.split()
            for index in range(len(temp_row)):
                temp_row[index] = int(temp_row[index])
            self.board.append(temp_row)

        self.dimensions = ( len(self.board), len(self.board[0]) )

    def print_board(self):
        for row in self.board:
            print row
        print self.dimensions

    def start_state(self):
        state = Node()
        for row in self.board:
            temp = []
            for space in row:
                temp.append(0)
            state.player_state.append(temp)


        return state

    def actions(self, state):
        actions = []
        for x in range(self.dimensions[0]):
            for y in range(self.dimensions[1]):
                if state.player_state[x][y] == 0:
                    actions.append( (x,y) )

        return actions

    def optimal_actions(self, state, player):
        actions = []
        fight_neighbors = []
        high_value = []
        alliance = []
        neighbor_free = []
        other_player = []

        total_val = 0
        count = 0
        for x in range(self.dimensions[0]):
            for y in range(self.dimensions[1]):
                count += 1
                value = self.board[x][y]
                total_val += value
                if state.player_state[x][y] == 0:
                    # Look at neighbors
                    neighbors = self.check_neighbors(state, x, y)
                    player1 = False
                    player2 = False

                    for item in neighbors:
                        if item == 1:
                            player1 = True
                        if item == 2:
                            player2 = True

                    if (player1 and player2):
                        fight_neighbors.append( (x,y) )
                    elif value > total_val / count:
                        high_value.append( (x,y) )
                    elif player == 1 and player1:
                        alliance.append( (x,y) )
                    elif player == 2 and player2:
                        alliance.append( (x,y) )
                    elif not (player1 and player2):
                        neighbor_free.append( (x,y) )
                    else:
                        other_player.append( (x,y) )


        actions = fight_neighbors
        #actions.extend(fight_neighbors)
        actions.extend(alliance)
        actions.extend(high_value)

        actions.extend(neighbor_free)
        actions.extend(other_player)

        return actions

    def check_neighbors(self, state, x, y):
        east = west = north = south = 0
        if x < self.dimensions[0] - 1:
            east = state.player_state[x+1][y]

        if x > 0:
            west = state.player_state[x-1][y]

        if y < self.dimensions[1] - 1:
            south = state.player_state[x][y+1]

        if y > 0:
            north = state.player_state[x][y-1]

        return [east, west, north, south]

    def result(self, state, action, player):
        # uses game rules
        # if not next self, then only that spot taken
        # else , that spot and neighbor spots with other player
        x = action[0]
        y = action[1]

        # Make a new state and deep copy
        new_state = Node()

        new_state.player_state = list(state.player_state)

        for row in range(len(new_state.player_state)):
            new_state.player_state[row] = list(state.player_state[row])


        # Take the spot picked
        new_state.player_state[x][y] = player

        north = south = east = west = None

        if x < self.dimensions[0] - 1:
            east = new_state.player_state[x+1][y]

        if x > 0:
            west = new_state.player_state[x-1][y]

        if y < self.dimensions[1] - 1:
            south = new_state.player_state[x][y+1]

        if y > 0:
            north = new_state.player_state[x][y-1]

        # Check to see if next to self
        if player in (east, west, north, south):
            if player == 1:
                if east == 2:
                    new_state.player_state[x+1][y] = 1
                if west == 2:
                    new_state.player_state[x-1][y] = 1
                if north == 2:
                    new_state.player_state[x][y-1] = 1
                if south == 2:
                    new_state.player_state[x][y+1] = 1

            if player == 2:
                if east == 1:
                    new_state.player_state[x+1][y] = 2
                if west == 1:
                    new_state.player_state[x-1][y] = 2
                if north == 1:
                    new_state.player_state[x][y-1] = 2
                if south == 1:
                    new_state.player_state[x][y+1] = 2

        return new_state


    def utility(self, state, player):
        # count up the territory held by each player then give difference
        player_1_total = 0
        player_2_total = 0

        for x in range(self.dimensions[0]):
            for y in range(self.dimensions[1]):
                if state.player_state[x][y] == 1:
                    player_1_total += self.board[x][y]
                else:
                    player_2_total += self.board[x][y]

        if player == 1:
            return player_1_total - player_2_total

        return player_2_total - player_1_total

    def score(self, state):
        # count up the territory held by each player then give difference
        player_1_total = 0
        player_2_total = 0

        for x in range(self.dimensions[0]):
            for y in range(self.dimensions[1]):
                if state.player_state[x][y] == 1:
                    player_1_total += self.board[x][y]
                else:
                    player_2_total += self.board[x][y]

        return [ player_1_total, player_2_total ]

    def evaluation(self, state, player):
        player_1_total = 0
        player_2_total = 0

        for x in range(self.dimensions[0]):
            for y in range(self.dimensions[1]):

                # Add up what the players have currently
                if state.player_state[x][y] == 1:
                    player_1_total += self.board[x][y]
                elif state.player_state[x][y] == 2:
                    player_2_total += self.board[x][y]

                # Give territory to closest player
                """else:
                    closest_player = self.check_closest_player(state, x, y)
                    if closest_player == 1:
                        player_1_total += self.board[x][y]
                    else:
                        player_2_total += self.board[x][y]"""

        if player == 1:
            return player_1_total - player_2_total

        return player_2_total - player_1_total

    def check_closest_player(self, state, x, y):
        last_player = 1
        last_dist = 1000000
        for xx in range(self.dimensions[0]):
            for yy in range(self.dimensions[1]):
                player = state.player_state[xx][yy]
                if player != 0:
                    dist = abs(xx-x) + abs(yy-y)
                    if dist < last_dist:
                        last_dist = dist
                        last_player = player

        return last_player
        #return random.randint(1,2)




""" Maybe make utility =  current player - other player """
def minimax_action(game, state, player, max_depth):
    # return the action that results in the max utility in the min val function
    best_action = ()
    best_utility = -sys.maxint
    nodes = 1
    total_nodes = 0

    for action in game.actions(state):
        result = min_value(game, game.result(state, action, player), player, max_depth)
        utility = result[0]
        total_nodes += result[1]
        #print result[1]

        if utility > best_utility:
            best_utility = utility
            best_action = action


    return [best_action, total_nodes]

def max_value(game, state, player, max_depth):
    if state.leaf():
        return [game.utility(state, player), 1]

    if max_depth == 1:
        return [game.evaluation(state, player), 1]

    utility = -sys.maxint

    total_nodes = 1
    for action in game.actions(state):

        result = min_value(game, game.result(state, action, player), player, max_depth - 1)
        total_nodes += result[1]
        utility = max(utility, result[0])

    return [utility, total_nodes]

def min_value(game, state, player, max_depth):
    if state.leaf():
        return [game.utility(state, player), 1]

    if max_depth == 1:
        return [game.evaluation(state, player), 1]

    utility = sys.maxint

    total_nodes = 1
    for action in game.actions(state):

        result = max_value(game, game.result(state, action, player), player, max_depth - 1)
        total_nodes += result[1]

        utility = min(utility, result[0])

    return [utility, total_nodes]

def alpha_beta_action(game, state, player, max_depth):
    # return the action that results in the max utility in the min val function
    best_action = ()
    best_utility = -sys.maxint
    nodes = 0

    for action in game.optimal_actions(state, player):

        result = ab_max_value(game, game.result(state, action, player), player, -sys.maxint, sys.maxint, max_depth)
        utility = result[0]
        nodes += result[1]
        #print result[1]
        if utility > best_utility:
            best_utility = utility
            best_action = action


    return [best_action, nodes]

def ab_max_value(game, state, player, alpha, beta, max_depth):
    if state.leaf():
        return [game.utility(state, player), 1]

    if max_depth == 1:
        return [game.evaluation(state, player), 1]

    utility = -sys.maxint
    nodes = 1
    for action in game.optimal_actions(state, player):

        result = ab_min_value(game, game.result(state, action, player), player, alpha, beta, max_depth - 1)

        utility = max(utility, result[0])

        nodes += result[1]

        if utility >= beta:
            return [utility, nodes]

        alpha = max(alpha, utility)

    return [utility, nodes]

def ab_min_value(game, state, player, alpha, beta, max_depth):
    if state.leaf():
        return [game.utility(state, player), 1]

    if max_depth == 1:
        return [game.evaluation(state, player), 1]

    utility = sys.maxint
    nodes = 1
    for action in game.optimal_actions(state, player):

        result = ab_max_value(game, game.result(state, action, player), player, alpha, beta, max_depth - 1)

        utility = min(utility, result[0])
        nodes += result[1]

        if utility <= alpha:
            return [utility, nodes]

        beta = min(beta, utility)

    return [utility, nodes]





def run_game(game):
    current_state = game.start_state()

    actins_taken = collections.deque()

    nodes_1 = 0
    nodes_2 = 0

    time_1 = 0
    time_2 = 0

    turns = 0

    finished = False

    while not finished:

        print "Player 1:"
        turns += 1

        start = time.clock()
        result = minimax_action(game, current_state, 1, 3)
        end = time.clock()
        action = result[0]
        actins_taken.append( ("Blue:", action) )
        nodes_1 += result[1]
        time_1 += end - start

        print "Average Nodes1: ", nodes_1/turns
        #print "This turn: ", result[1]
        print "Ave time1: ", time_1/turns
        #print "This turn: ", end - start

        current_state = game.result(current_state, action, 1)

        #current_state.print_state()

        finished = current_state.leaf()

        if finished:
            break

        print "Player 2:"

        start = time.clock()
        result = alpha_beta_action(game, current_state, 2, 5)
        end = time.clock()
        action = result[0]
        actins_taken.append( ("Green:", action) )
        nodes_2 += result[1]
        time_2 += end - start

        print "Average Nodes2: ", nodes_2/turns
        #print "This turn: ", result[1]
        print "Ave time2: ", time_2/turns
        #print "This turn: ", end - start

        current_state = game.result(current_state, action, 2)

        #current_state.print_state()

        finished = current_state.leaf()

    score = game.score(current_state)
    current_state.print_state()
    print "Score 1: ", score[0]
    print "Score 2: ", score[1]
    print nodes_1
    print nodes_2
    print actins_taken



myGame = Wargame("../boards/Sevastopol.txt")


run_game(myGame)