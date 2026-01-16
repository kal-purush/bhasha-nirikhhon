class GameLogic:

    def __init__(self):
        self.win_combinations = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [0, 3, 6],
            [1, 4, 7],
            [2, 5, 8],
            [0, 4, 8],
            [2, 4, 6]
        ]

    def is_winner(self, board):
        for combination in self.win_combinations:
            pos_1, pos_2, pos_3 = combination

            outcome = board[pos_1] == board[pos_2] and board[pos_2] == board[pos_3]

            if outcome:
                return True

        return False

    def is_tie(self, board):
        return self.__is_board_full(board) and not self.is_winner(board)

    def __is_board_full(self, board):

        for pos in board:
            if type(pos) == int:
                return False

        return True