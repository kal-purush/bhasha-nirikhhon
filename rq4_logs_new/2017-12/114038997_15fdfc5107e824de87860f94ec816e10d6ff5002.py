from Grid import*
import random

"""This class implements the logic for Conways Game of life. Conways game
of life is a cellular automation developed by John Conway in 1970. A cell
is either alive or empty in a grid. The grid is then updated such that any
cell that is alive with two or three adjacent live cells stays alive, any
other live cell dies and vacates its cell leaving it empty. A cell that is
empty is born if it has exactly three adjacent cells that are alive, any
other cell that does not meet such condition stays empty."""
class ConwaysLogic:
  SURVIVE = [2,3]
  BORN = 3
  TRAITS = {"Value":False}

  #The __init__ function  sets up the simuation. It creates a Grid object to
  #store the information for the individual cells and the grid as a whole.
  #It receives either no parameters or a two dimensional array which describes
  #which cells are supposed to be born in the beginning. If there is no array
  #then it is assumed that the entire board is empty
  def __init__(self,width,height,*board):
    self.__board = Grid(width,height,ConwaysLogic.TRAITS)
    self.__width = width
    self.__height = height
    if board:
      for i in range(0,width):
        for j in range(0,height):
          self.__board.setValue(i,j,"Value",board[i][j])

  #This function updates the board. It creates a new ConwaysLogic object that
  #is empty and populates it based on the conditions stated in the above
  #block comment, using the calling object as the basis for the next itteration.
  #After the new ConwaysLogic board is populated, the former ConwaysLogic
  #object is updated such that it's board's values are the sameas the new
  #ConwaysLogic object using the Grid class's updateTraitArray method. Once the
  #ConwaysLogic object is finally updated, a two dimensional array with the
  #values of the updated ConwaysLogic object is returned by calling the getBoard
  #method
  def updateBoard(self):
    newBoard = ConwaysLogic(self.__width,self.__height)
    changedValues = []
    for i in range(0,self.__width):
      for j in range(0,self.__height):
        numAdj = self.__board.getNumAdj(i,j,"Value",True,True)
        if self.__board.getValue(i,j,"Value"):
          value = numAdj in ConwaysLogic.SURVIVE
          newBoard.__board.setValue(i,j,"Value",value)
          if not value:
            changedValues.append({"x":i,"y":j})
        else:
          value = numAdj == ConwaysLogic.BORN
          newBoard.__board.setValue(i,j,"Value",value)
          if value:
            changedValues.append({"x":i,"y":j})          
    self.__board.updateTraitArray("Value",\
                                  newBoard.__board.getTraitArray("Value"))
    return changedValues
  #This function is used to generate a random conways board. Each individual
  #cell has a 20% chance of becoming born in the random board. RAN_RANGE
  #is defined as 4 so that this provides the correct probability for each
  #cell to be born
  def randomize(self):
    RAN_RANGE = 4
    for i in range(0,self.__width):
      for j in range(0,self.__height):
        ranNum = random.randint(0,RAN_RANGE)
        if ranNum == ConwaysLogic.BORN:
          self.__board.setValue(i,j,"Value",True)
        else:
          self.__board.setValue(i,j,"Value",False)
    return self.getBoard()
  #This function changes the value of the cell to the opposite of what it is
  def changeValue(self,x,y):
    value = not self.__board.getValue(x,y,"Value")
    self.__board.setValue(x,y,"Value",value)
    return value
  def getValue(self,x,y):
    return self.__board.getValue(x,y,"Value")
  #This function returns a two dimensional array that describes the current state
  #of the board.
  
  def getBoard(self):
    return self.__board.getTraitArray("Value")
"""
connor = ConwaysLogic()
connor.randomize()
print("\n")
for i in range(0,100):
  for i in range(0,10):
    newStr = ""
    for j in range(0,10):
      if connor.getBoard()[i][j]:
        newStr += "X|"
      else:
        newStr += " |"
    print(newStr)
  print("*|*|*|*|*|*|*|*|*|*|")
  connor.updateBoard()
"""