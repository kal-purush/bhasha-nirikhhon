# m, n = 8, 5 # 8 by 5 grid
# index = 12


# for index in range(40):
#   i, j = index % m + 1, index // m + 1
#   print("board[{}] is ({}, {})".format(index, i, j))

# (5, 2) -> 12?
# (2 - 1) * m + 


# for name in ["Alfredo", "Spaghetti", "Fettuccine"]:
#   print(name)

# range(40) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12, ...., 39 ]
for index in range(40):
  print(index)

# for THING in LIST:
#     DO STUFF

number = 0
while 0 < 40:
  print(number)
  number = number + 1
  #number += 1

  # while CONDITION:
  #   DO STUFF

# Battleship:
# 
## Battleship Game
# At the start of the game:
# 1. Place all the ships
#
# while NO ONE HAS WON:
  # Each turn:
  # 1. Opponent asks "Where do you want to fire?"
  # 2. You answer with a coordinate.
  # 3. You fire then!
  # 4. They tell you whether you hit or not, or sunk a ship. 
  # 5. Switch who the opponent is
#