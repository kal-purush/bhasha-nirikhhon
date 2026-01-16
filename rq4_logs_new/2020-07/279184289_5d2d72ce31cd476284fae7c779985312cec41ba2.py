class TicTacToeGame():
    def __init__(self):
        self.board = [[0,0,0],
                [0,0,0],
                [0,0,0]]

    def make_move(self, player, position):
        '''
        player: 'x' or 'o'
        position: tuple 

        Takes a player and position, checks that the position is open
        and then updates the board
        '''

        x,y = position
        if self.board[x][y] == 0:
            if player == 'x':
                self.board[x][y] = 1
            elif player == 'o':
                self.board[x][y] = -1

    def check_for_winner(self):
        '''
        Checks all possible ways to win and returns the winning player
        If no winning player, returns nothing
        '''

        for i in range(3):
            # First check the rows
            if sum(self.board[i]) == 3:
                return 'x'
            if sum(self.board[i]) == -3:
                return 'o'
            # Then check the columns
            if sum([row[i] for row in self.board]) == 3:
                return 'x'
            if sum([row[i] for row in self.board]) == -3:
                return 'o'

        #Then check the diagonals
        if sum([self.board[i][i] for i in range(3)]) == 3:
            return 'x'
        if sum([self.board[i][i] for i in range(3)]) == -3:
            return 'o'
        if sum([self.board[i][2-i] for i in range(3)]) == 3:
            return 'x'
        if sum([self.board[i][2-i] for i in range(3)]) == -3:
            return 'o'
