from core_tic_tac_toe.game import Game
from core_tic_tac_toe.human_player import HumanPlayer
from core_tic_tac_toe.easy_computer_player import EasyComputerPlayer


class GameConfig:

    def __init__(self, board, display, game_logic, input_validator, menu):
        self.board = board
        self.display = display
        self.game_logic = game_logic
        self.input_validator = input_validator
        self.menu = menu
        self.game_options = "1. Human vs Human \n2. Human vs Easy Computer"

    def prepare_players(self, player_selection):

        if player_selection == "1":
            self.__prepare_human_players()
        elif player_selection == "2":
            self.__prepare_human_and_easy_computer_players()

    def prepare_game(self):
        self.display.welcome_message()
        game_selection = self.menu.game_selection(self.game_options)

        self.prepare_players(game_selection)

        game = Game(board=self.board, display=self.display,
                    game_logic=self.game_logic, player_1=self.player_1, player_2=self.player_2)

        return game

    def __prepare_human_players(self):
        self.player_1 = HumanPlayer(name="Player 1", symbol="X", board=self.board,
                                    display=self.display, input_validator=self.input_validator)

        self.player_2 = HumanPlayer(name="Player 2", symbol="O", board=self.board,
                                    display=self.display, input_validator=self.input_validator)

    def __prepare_human_and_easy_computer_players(self):
        self.player_1 = HumanPlayer(name="Player 1", symbol="X", board=self.board,
                                    display=self.display, input_validator=self.input_validator)

        self.player_2 = EasyComputerPlayer(
            name="Easy Computer", player_marker="O", board=self.board)