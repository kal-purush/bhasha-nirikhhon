from core_tic_tac_toe.human_player import HumanPlayer
from core_tic_tac_toe.board import Board


class Menu:

    def __init__(self, display, input_validator):
        self.display = display
        self.input_validator = input_validator
        self.menu_options = {
            "1": "1. Human vs Human"
        }

    def game_options(self):

        self.display.welcome_message()

        all_game_options = (f'{self.menu_options["1"]}')

        self.display.game_options(all_game_options)

    def game_selection(self):
        self.game_options()

        game_option = self.display.get_player_input()

        if self.input_validator.is_game_option_valid(game_option):
            return game_option
        else:
            return self.game_selection()

    def prep_players(self):
        selection = self.game_selection()
        board = Board()

        if selection == "1":
            player_1 = HumanPlayer(name="Player 1", symbol="X", board=board,
                                   display=self.display, input_validator=self.input_validator)
            player_2 = HumanPlayer(name="Player 2", symbol="O", board=board,
                                   display=self.display, input_validator=self.input_validator)

        return [player_1, player_2]