import serial
import time
import re
import json
import pickle
from lib import sunfish as ai
from lib import board

arduino = Board('COM9', 9600) # initialize arduino board

def print_pos(pos):
    print(' '.join(pos.board))

def main():
    pos = ai.Position(ai.initial, 0, (True,True), (True,True), 0, 0)
    searcher = ai.Searcher()

    while True:
        print_pos(pos)

        if pos.score <= -ai.MATE_LOWER:
            print("You lost")
            break

        move = None

        while move not in pos.gen_moves():
            match = re.match('([a-h][1-8])'*2, str(input('Your move: ')).lower())

            if match:
                move = ai.parse(match.group(1)), ai.parse(match.group(2))
            else:
                print("Invalid Move !")
                print("")

        pos = pos.move(move)

        # After our move we rotate the board and print it again.
        # This allows us to see the effect of our move.
        print_pos(pos.rotate())

        if pos.score <= -ai.MATE_LOWER:
            print("You won")
            break

        # Fire up the engine to look for a move.
        move, score = searcher.search(pos, secs=1)

        if score == ai.MATE_UPPER:
            print("Checkmate!")

        moves = [str(ai.render(119-move[0])).upper(), str(ai.render(119-move[1])).upper()]

        print("AI move: {0}{1}".format(moves[0], moves[1]))

        if (pos.board[move[1]] != '.'):
            print("AI just destroyed your :" + pos.board[move[1]])

            # first remove the pone
            arduino.make_move(moves[1])

            # now move pone to the new place
            arduino.make_move(moves[0], moves[1])

        else:
            arduino.make_move(moves[0], moves[1])

        pos = pos.move(move)

main()