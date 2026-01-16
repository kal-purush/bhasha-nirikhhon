from tictactoe import TicTacToeGame
import ast

if __name__ == '__main__':
    tt = TicTacToeGame()

    winner = None
    players = ['x', 'o']
    current_player = 1

    while winner is None:
        current_player = 1 - current_player

        position = input('Input {}\'s move:'.format(players[current_player]))
        position = ast.literal_eval(position)
        tt.make_move(player = players[current_player], position = position)

        tt.pretty_print_board()

        winner = tt.check_for_winner()
        
    print('{} wins'.format(players[current_player]))