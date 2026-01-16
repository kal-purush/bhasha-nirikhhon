from core_tic_tac_toe.board import Board
from core_tic_tac_toe.display import Display
from core_tic_tac_toe.io import Io
from core_tic_tac_toe.easy_computer_player import EasyComputerPlayer
from core_tic_tac_toe.human_player import HumanPlayer
from core_tic_tac_toe.game import Game
from core_tic_tac_toe.game_logic import GameLogic

if __name__ == "__main__":
    io = Io()
    display = Display(io)
    board = Board()
    game_logic = GameLogic()
    player_1 = HumanPlayer("Player 1", "X", board, display)
    player_2 = EasyComputerPlayer("Easy Computer Player", "O", board)

    game = Game(display=display, board=board,
                player_1=player_1, player_2=player_2, game_logic=game_logic)

    game.start()