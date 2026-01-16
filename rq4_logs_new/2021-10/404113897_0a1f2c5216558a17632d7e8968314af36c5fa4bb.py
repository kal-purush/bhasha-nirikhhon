class Game:

    def __init__(self, board, display, game_logic, player_1, player_2):
        self.board = board
        self.display = display
        self.game_logic = game_logic
        self.player_1 = player_1
        self.player_2 = player_2
        self.current_player = self.player_1

    def start(self):
        while True:
            self.__show_board()

            self.__prompt_move(self.get_current_player())

            moves = self.__get_board()

            if self.__is_game_over(moves):
                break
            else:
                self.__switch_player()

        self.__show_board()

    def get_current_player(self):
        return self.current_player

    def get_current_player_name(self):
        return self.current_player.get_name()

    def __show_board(self):
        board = self.__get_board()

        self.display.print_board(board)

    def __prompt_move(self, current_player):
        self.display.print_player_turn(self.get_current_player_name())

        current_player.make_move()

    def __is_game_over(self, moves):
        if self.game_logic.check_winner(moves):
            self.display.announce_winner(self.get_current_player_name())
            return True

        if self.game_logic.is_tie(moves):
            self.display.announce_tie()
            return True

    def __switch_player(self):
        if self.current_player == self.player_1:
            self.current_player = self.player_2
        else:
            self.current_player = self.player_1

    def __get_board(self):
        return self.board.get_board()