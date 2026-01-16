# name = input("What is your name?")
# print("Your name is {}".format(name))

# Battleship Game
# At the start of the game:
# 1. Place all the ships
#
# Each turn:
# 1. Opponent asks "Where do you want to fire?"
# 2. You answer with a coordinate.
# 3. You fire then!
# 4. They tell you whether you hit or not, or sunk a ship.
# coords 0  1  2  3  4  5  6  7  8

# each turn
board = [0, 0, 0, 0, 0, 1, 0, 1, 0]
num_ships = 2

coordinate = int(input("Where do you want to fire?"))
print("You fired at {}".format(coordinate))
if board[coordinate] == 1:
    print("You hit!")
else:  # not board[coordinate] == 1
    print("You missed!")