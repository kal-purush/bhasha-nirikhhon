import math


class HardComputerPlayer:
    def __init__(self, name, symbol, opponent_symbol, board, game_logic):
        self.__name = name
        self.__symbol = symbol
        self.__opponent_symbol = opponent_symbol
        self.__board = board
        self.__game_logic = game_logic

    @property
    def name(self):
        return self.__name

    @property
    def symbol(self):
        return self.__symbol

    @property
    def opponent_symbol(self):
        return self.__opponent_symbol

    @property
    def board(self):
        return self.__board

    @property
    def game_logic(self):
        return self.__game_logic

    def make_move(self):
        best_score = -math.inf
        best_move = None

        available_moves = self.board.get_available_positions()

        for move in available_moves:
            self.board.execute_move(move - 1, self.symbol)
            score = self.minimax(0, False)
            self.board.undo_execution(move)

            if score > best_score:
                best_score = score
                best_move = move

        self.board.execute_move(best_move - 1, self.symbol)

    def minimax(self, depth, is_maximizer):
        board_state = self.board.get_board()
        available_moves = self.board.get_available_positions()

        if self.game_logic.check_winner(board_state, self.symbol):
            return 10 - depth
        elif self.game_logic.check_winner(board_state, self.opponent_symbol):
            return -10 + depth
        elif len(available_moves) == 0:
            return 0

        if is_maximizer:
            best_score = -math.inf
            for move in available_moves:
                self.board.execute_move(move - 1, self.symbol)
                score = self.minimax(depth+1, False)
                self.board.undo_execution(move)
                best_score = max(score, best_score)

            return best_score
        else:
            best_score = math.inf
            for move in available_moves:
                self.board.execute_move(move - 1, self.opponent_symbol)
                score = self.minimax(depth+1, True)
                self.board.undo_execution(move)
                best_score = min(score, best_score)

            return best_score